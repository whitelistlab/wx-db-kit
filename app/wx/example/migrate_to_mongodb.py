import sqlite3
from datetime import datetime
import os
import sys
from typing import List, Dict
from pymongo import MongoClient
from bson import ObjectId

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from app.util.protocbuf.msg_pb2 import MessageBytesExtra


# MongoDB连接配置
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "wechat_msg"

class ChatDataMigrator:
    def __init__(self, msg_db_path: str = "./app/DataBase/Msg/MSG.db", contact_db_path: str = "./app/DataBase/Msg/MicroMsg.db",
                 mongo_uri: str = MONGO_URI, db_name: str = DB_NAME):
        self.msg_db_path = msg_db_path
        self.contact_db_path = contact_db_path
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.msg_conn = None
        self.msg_cursor = None
        self.contact_conn = None
        self.contact_cursor = None
        self.mongo_client = None
        self.db = None

    def connect_databases(self) -> bool:
        """连接SQLite和MongoDB数据库，并创建必要的集合和索引"""
        try:
            # 连接SQLite数据库
            if not os.path.exists(self.msg_db_path) or not os.path.exists(self.contact_db_path):
                print(f"错误：数据库文件不存在: {self.msg_db_path} 或 {self.contact_db_path}")
                return False

            self.msg_conn = sqlite3.connect(self.msg_db_path)
            self.msg_cursor = self.msg_conn.cursor()
            self.contact_conn = sqlite3.connect(self.contact_db_path)
            self.contact_cursor = self.contact_conn.cursor()

            # 连接MongoDB
            self.mongo_client = MongoClient(self.mongo_uri)
            self.db = self.mongo_client[self.db_name]
            
            # 创建集合和索引
            # 用户集合
            self.db.users.create_index('wxid', unique=True)
            
            # 群组集合
            self.db.groups.create_index('group_id', unique=True)
            # self.db.groups.create_index('owner_id')
            
            # 消息集合
            self.db.messages.create_index([('chat_id', 1), ('create_time', -1)])
            self.db.messages.create_index([('sender_id', 1), ('create_time', -1)])
            self.db.messages.create_index([('receiver_id', 1), ('create_time', -1)])
            
            return True
        except Exception as e:
            print(f"连接数据库失败：{e}")
            return False

    def get_contact_info(self, wxid: str) -> Dict:
        """获取联系人信息"""
        try:
            sql = '''
                SELECT Contact.UserName, Contact.NickName, Contact.Remark, Contact.Type, Contact.Alias,
                       ContactHeadImgUrl.smallHeadImgUrl
                FROM Contact
                LEFT JOIN ContactHeadImgUrl ON Contact.UserName = ContactHeadImgUrl.usrName
                WHERE Contact.UserName = ?
            '''
            self.contact_cursor.execute(sql, (wxid,))
            result = self.contact_cursor.fetchone()
            
            if result:
                return {
                    'wxid': result[0],
                    'nickname': result[1],
                    'remark': result[2],
                    'type': result[3],
                    'alias': result[4],
                    'small_head_img_url': result[5],
                    'is_group': wxid.endswith('@chatroom')
                }
            return None
        except sqlite3.Error as e:
            print(f"获取联系人信息失败：{e}")
            return None

    def process_message(self, msg: tuple) -> Dict:
        """处理单条消息记录"""
        create_time, talker, is_sender, msg_type, content, bytes_extra = msg
        
        message_data = {
            'create_time': datetime.fromtimestamp(create_time),
            'sender_wxid': talker if not is_sender else 'self',
            'receiver_wxid': 'self' if not is_sender else talker,
            'is_sender': bool(is_sender),
            'msg_type': msg_type,
            'content': content,
            'is_group_msg': talker.endswith('@chatroom')
        }

        # 处理群消息发送者信息
        if message_data['is_group_msg'] and bytes_extra:
            try:
                msgbytes = MessageBytesExtra()
                msgbytes.ParseFromString(bytes_extra)
                for tmp in msgbytes.message2:
                    if tmp.field1 == 1:
                        group_sender_wxid = tmp.field2
                        if ':' in group_sender_wxid:
                            group_sender_wxid = group_sender_wxid.split(':')[0]
                        # 更新消息的发送者为群成员wxid
                        if not is_sender:
                            message_data['sender_wxid'] = group_sender_wxid
                        
                        # 获取群消息发送者的详细信息
                        sender_info = self.get_contact_info(group_sender_wxid)
                        if sender_info:
                            message_data['sender_info'] = {
                                'nickname': sender_info['nickname'],
                                'remark': sender_info['remark'],
                                'alias': sender_info['alias']
                            }
                        break
            except Exception as e:
                print(f"解析群消息发送者信息失败：{e}")

        return message_data

    def migrate_data(self):
        """迁移数据到MongoDB，按照三范式设计拆分数据"""
        try:
            # 获取所有联系人
            sql = '''
                SELECT DISTINCT StrTalker
                FROM MSG
                ORDER BY CreateTime DESC
            '''
            self.msg_cursor.execute(sql)
            contacts = [row[0] for row in self.msg_cursor.fetchall()]

            # 迁移联系人信息到用户集合
            for wxid in contacts:
                contact_info = self.get_contact_info(wxid)
                if contact_info:
                    if contact_info['is_group']:
                        # 群组信息
                        group_doc = {
                            'group_id': contact_info['wxid'],
                            'name': contact_info['nickname'],
                            # 'owner_id': None,  # 需要从其他地方获取群主信息
                            'create_time': datetime.now(),
                            'update_time': datetime.now()
                        }
                        self.db.groups.update_one(
                            {'group_id': group_doc['group_id']},
                            {'$set': group_doc},
                            upsert=True
                        )
                    else:
                        # 普通用户信息
                        user_doc = {
                            'wxid': contact_info['wxid'],
                            'nickname': contact_info['nickname'],
                            'remark': contact_info['remark'],
                            'type': contact_info['type'],
                            'alias': contact_info['alias'],
                            'small_head_img_url': contact_info['small_head_img_url'],
                            'create_time': datetime.now(),
                            'update_time': datetime.now()
                        }
                        self.db.users.update_one(
                            {'wxid': user_doc['wxid']},
                            {'$set': user_doc},
                            upsert=True
                        )

            # 迁移消息记录
            for wxid in contacts:
                sql = '''
                    SELECT 
                        CreateTime,
                        StrTalker,
                        IsSender,
                        Type,
                        StrContent,
                        BytesExtra
                    FROM MSG
                    WHERE StrTalker = ?
                    ORDER BY CreateTime ASC
                '''
                self.msg_cursor.execute(sql, (wxid,))
                messages = self.msg_cursor.fetchall()

                for msg in messages:
                    create_time, talker, is_sender, msg_type, content, bytes_extra = msg
                    
                    # 消息记录
                    message_doc = {
                        'chat_id': talker,
                        'sender_id': 'self' if is_sender else talker,
                        'type': msg_type,
                        'content': content,
                        'create_time': datetime.fromtimestamp(create_time),
                        'nickname': None,  # 发送者昵称
                        'remark': None     # 发送者备注名
                    }
                    
                    # 获取发送者的昵称和备注信息
                    if not is_sender:  # 如果不是自己发送的消息
                        sql = '''
                            SELECT NickName, Remark
                            FROM Contact
                            WHERE UserName = ?
                        '''
                        self.contact_cursor.execute(sql, (talker,))
                        result = self.contact_cursor.fetchone()
                        if result:
                            message_doc['nickname'] = result[0]
                            message_doc['remark'] = result[1]
                    
                    # 处理群消息发送者信息
                    if talker.endswith('@chatroom') and bytes_extra:
                        try:
                            msgbytes = MessageBytesExtra()
                            msgbytes.ParseFromString(bytes_extra)
                            for tmp in msgbytes.message2:
                                if tmp.field1 == 1:
                                    group_sender_wxid = tmp.field2
                                    if ':' in group_sender_wxid:
                                        group_sender_wxid = group_sender_wxid.split(':')[0]
                                    # 更新消息的发送者为群成员wxid
                                    if not is_sender:
                                        message_doc['sender_id'] = group_sender_wxid
                                        # 获取群消息发送者的昵称和备注信息
                                        sql = '''
                                            SELECT NickName, Remark
                                            FROM Contact
                                            WHERE UserName = ?
                                        '''
                                        self.contact_cursor.execute(sql, (group_sender_wxid,))
                                        result = self.contact_cursor.fetchone()
                                        if result:
                                            message_doc['nickname'] = result[0]
                                            message_doc['remark'] = result[1]
                                    break
                        except Exception as e:
                            print(f"解析群消息发送者信息失败：{e}")
                    
                    # 保存消息记录
                    self.db.messages.update_one(
                        {'chat_id': message_doc['chat_id'], 'create_time': message_doc['create_time']},
                        {'$set': message_doc},
                        upsert=True
                    )

            print("数据迁移完成")

        except Exception as e:
            print(f"数据迁移失败：{e}")
            raise e

    def close(self):
        """关闭数据库连接"""
        if self.msg_cursor:
            self.msg_cursor.close()
        if self.msg_conn:
            self.msg_conn.close()
        if self.contact_cursor:
            self.contact_cursor.close()
        if self.contact_conn:
            self.contact_conn.close()
        if self.mongo_client:
            self.mongo_client.close()

def main():
    migrator = ChatDataMigrator()
    
    if not migrator.connect_databases():
        return

    try:
        migrator.migrate_data()
    finally:
        migrator.close()

if __name__ == "__main__":
    main()