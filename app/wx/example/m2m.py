import os
import sqlite3
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from typing import Dict, List
from app.util.protocbuf.msg_pb2 import MessageBytesExtra

class MongoDBMigrator:
    def __init__(self, mongodb_uri: str = "mongodb://localhost:27017/", database_name: str = "wechat_msg"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.msg_collection = self.db.messages
        self.contact_collection = self.db.contacts
        self.chatroom_collection = self.db.chatrooms
        self._create_indexes()
    
    def _create_indexes(self):
        """创建必要的索引以优化查询性能"""
        # 消息集合的索引
        self.msg_collection.create_index([("str_talker", ASCENDING), ("create_time", ASCENDING)])
        self.msg_collection.create_index("msg_svr_id")
        
        # 联系人集合的索引
        self.contact_collection.create_index("username", unique=True)
        
        # 群聊集合的索引
        self.chatroom_collection.create_index("chatroom_name", unique=True)
    
    def migrate_messages(self, msg_db_path: str):
        """迁移消息数据"""
        if not os.path.exists(msg_db_path):
            raise FileNotFoundError(f"MSG数据库文件不存在: {msg_db_path}")
        
        conn = sqlite3.connect(msg_db_path)
        cursor = conn.cursor()
        
        # 分批获取消息数据
        batch_size = 1000
        offset = 0
        
        while True:
            cursor.execute(
                """SELECT localId, TalkerId, Type, SubType, IsSender, CreateTime, 
                   Status, StrContent, MsgSvrID, BytesExtra, StrTalker, CompressContent, 
                   DisplayContent 
                   FROM MSG LIMIT ? OFFSET ?""", 
                (batch_size, offset)
            )
            rows = cursor.fetchall()
            if not rows:
                break
                
            messages_to_insert = []
            for row in rows:
                message = {
                    "local_id": row[0],
                    "talker_id": row[1],
                    "type": row[2],
                    "sub_type": row[3],
                    "is_sender": bool(row[4]),
                    "create_time": datetime.fromtimestamp(row[5]),
                    "status": row[6],
                    "str_content": row[7],
                    "msg_svr_id": row[8],
                    "bytes_extra": row[9],
                    "str_talker": row[10],
                    "compress_content": row[11],
                    "display_content": row[12],
                    "user_name": ""
                }
                
                # 解析bytes_extra字段获取user_name
                if row[9] and not bool(row[4]):  # 只解析非自己发送的消息
                    try:
                        msgbytes = MessageBytesExtra()
                        msgbytes.ParseFromString(row[9])
                        for tmp in msgbytes.message2:
                            if tmp.field1 == 1:
                                message["user_name"] = tmp.field2
                                break
                    except Exception:
                        pass  # 解析失败时保持user_name为空字符串
                
                messages_to_insert.append(message)
            
            if messages_to_insert:
                self.msg_collection.insert_many(messages_to_insert)
            
            offset += batch_size
        
        conn.close()
    
    def migrate_contacts(self, micromsg_db_path: str):
        """迁移联系人数据"""
        if not os.path.exists(micromsg_db_path):
            raise FileNotFoundError(f"MicroMsg数据库文件不存在: {micromsg_db_path}")
        
        conn = sqlite3.connect(micromsg_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT UserName, Alias, Type, Remark, NickName, PYInitial, 
                   RemarkPYInitial, ContactHeadImgUrl.smallHeadImgUrl, 
                   ContactHeadImgUrl.bigHeadImgUrl, ExTraBuf
            FROM Contact
            INNER JOIN ContactHeadImgUrl ON Contact.UserName = ContactHeadImgUrl.usrName
            WHERE Type != 4 AND VerifyFlag = 0 AND NickName != ''
        """)
        
        contacts = cursor.fetchall()
        contacts_to_insert = []
        
        for contact in contacts:
            contact_doc = {
                "username": contact[0],
                "alias": contact[1],
                "type": contact[2],
                "remark": contact[3],
                "nickname": contact[4],
                "py_initial": contact[5],
                "remark_py_initial": contact[6],
                "small_head_img_url": contact[7],
                "big_head_img_url": contact[8],
                "extra_buf": contact[9]
            }
            contacts_to_insert.append(contact_doc)
        
        if contacts_to_insert:
            self.contact_collection.insert_many(contacts_to_insert)
        
        conn.close()
    
    def migrate_chatrooms(self, micromsg_db_path: str):
        """迁移群聊数据"""
        if not os.path.exists(micromsg_db_path):
            raise FileNotFoundError(f"MicroMsg数据库文件不存在: {micromsg_db_path}")
        
        conn = sqlite3.connect(micromsg_db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT ChatRoomName, RoomData FROM ChatRoom")
        chatrooms = cursor.fetchall()
        
        chatrooms_to_insert = []
        for chatroom in chatrooms:
            chatroom_doc = {
                "chatroom_name": chatroom[0],
                "room_data": chatroom[1]
            }
            chatrooms_to_insert.append(chatroom_doc)
        
        if chatrooms_to_insert:
            self.chatroom_collection.insert_many(chatrooms_to_insert)
        
        conn.close()

def migrate_to_mongodb(msg_db_path: str, micromsg_db_path: str, mongodb_uri: str = "mongodb://localhost:27017/", database_name: str = "wechat_msg"):
    """主函数：执行完整的数据迁移过程"""
    migrator = MongoDBMigrator(mongodb_uri, database_name)
    
    print("开始迁移消息数据...")
    migrator.migrate_messages(msg_db_path)
    print("消息数据迁移完成")
    
    print("开始迁移联系人数据...")
    migrator.migrate_contacts(micromsg_db_path)
    print("联系人数据迁移完成")
    
    print("开始迁移群聊数据...")
    migrator.migrate_chatrooms(micromsg_db_path)
    print("群聊数据迁移完成")
    
    print("所有数据迁移完成！")

if __name__ == "__main__":
    # 示例用法
    MSG_DB_PATH = "./app/Database/Msg/MSG.db"
    MICROMSG_DB_PATH = "./app/Database/Msg/MicroMsg.db"
    
    migrate_to_mongodb(
        msg_db_path=MSG_DB_PATH,
        micromsg_db_path=MICROMSG_DB_PATH,
        mongodb_uri="mongodb://localhost:27017/",
        database_name="wechat_msg"
    )