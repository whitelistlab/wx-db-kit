import os
import sqlite3
from datetime import datetime
from typing import Dict
import time
import schedule
from pymongo import MongoClient, ASCENDING

from app.util.protocbuf.msg_pb2 import MessageBytesExtra
from app.wx.example.decrypt import decrypt_db, merge_databases_wrapper
from app.wx.wx_decrypt import get_wechat_info



class MongoDBMigrator:
    def __init__(self, mongodb_uri: str = "mongodb://localhost:27017/", database_name: str = "wechat_msg"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.msg_collection = self.db.messages
        self.contact_collection = self.db.contacts
        self.chatroom_collection = self.db.chatrooms
        self.sync_status_collection = self.db.sync_status
        self._create_indexes()
    
    def _create_indexes(self):
        """创建必要的索引以优化查询性能"""
        # 消息集合的索引
        self.msg_collection.create_index([('str_talker', ASCENDING), ('create_time', ASCENDING)])
        self.msg_collection.create_index('msg_svr_id')
        self.msg_collection.create_index('local_id')
        
        # 联系人集合的索引
        self.contact_collection.create_index('username', unique=True)
        self.contact_collection.create_index('last_update_time')
        
        # 群聊集合的索引
        self.chatroom_collection.create_index('chatroom_name', unique=True)
        self.chatroom_collection.create_index('last_update_time')
    
    def _get_last_sync_status(self, collection_name: str) -> Dict:
        """获取最后同步状态"""
        status = self.sync_status_collection.find_one({'collection': collection_name})
        if not status:
            status = {
                'collection': collection_name,
                'last_local_id': 0,
                'last_sync_time': 0
            }
            self.sync_status_collection.insert_one(status)
        return status
    
    def _update_sync_status(self, collection_name: str, last_local_id: int):
        """更新同步状态"""
        self.sync_status_collection.update_one(
            {'collection': collection_name},
            {'$set': {
                'last_local_id': last_local_id,
                'last_sync_time': int(time.time())
            }}
        )
    
    def migrate_messages(self, msg_db_path: str):
        """增量迁移消息数据"""
        if not os.path.exists(msg_db_path):
            raise FileNotFoundError(f"MSG数据库文件不存在: {msg_db_path}")
        
        # 获取上次同步状态
        status = self._get_last_sync_status('messages')
        last_local_id = status['last_local_id']
        
        conn = sqlite3.connect(msg_db_path)
        cursor = conn.cursor()
        
        # 分批获取新增消息数据
        batch_size = 1000
        while True:
            cursor.execute(
                """SELECT localId, TalkerId, Type, SubType, IsSender, CreateTime, 
                   Status, StrContent, MsgSvrID, BytesExtra, StrTalker, CompressContent, 
                   DisplayContent 
                   FROM MSG 
                   WHERE localId > ? 
                   ORDER BY localId 
                   LIMIT ?""", 
                (last_local_id, batch_size)
            )
            rows = cursor.fetchall()
            if not rows:
                break
                
            messages_to_insert = []
            for row in rows:
                message = {
                    'local_id': row[0],
                    'talker_id': row[1],
                    'type': row[2],
                    'sub_type': row[3],
                    'is_sender': bool(row[4]),
                    'create_time': datetime.fromtimestamp(row[5]),
                    'status': row[6],
                    'str_content': row[7],
                    'msg_svr_id': row[8],
                    'bytes_extra': row[9],
                    'str_talker': row[10],
                    'compress_content': row[11],
                    'display_content': row[12],
                    'user_name': ''
                }
                
                # 解析bytes_extra字段获取user_name
                if row[9] and not bool(row[4]):
                    try:
                        msgbytes = MessageBytesExtra()
                        msgbytes.ParseFromString(row[9])
                        for tmp in msgbytes.message2:
                            if tmp.field1 == 1:
                                message['user_name'] = tmp.field2
                                break
                    except Exception:
                        pass
                
                messages_to_insert.append(message)
                last_local_id = row[0]
            
            if messages_to_insert:
                self.msg_collection.insert_many(messages_to_insert)
                self._update_sync_status('messages', last_local_id)
        
        conn.close()
    
    def migrate_contacts(self, micromsg_db_path: str):
        """增量迁移联系人数据"""
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
        current_time = datetime.now()
        
        for contact in contacts:
            contact_doc = {
                'username': contact[0],
                'alias': contact[1],
                'type': contact[2],
                'remark': contact[3],
                'nickname': contact[4],
                'py_initial': contact[5],
                'remark_py_initial': contact[6],
                'small_head_img_url': contact[7],
                'big_head_img_url': contact[8],
                'extra_buf': contact[9],
                'last_update_time': current_time
            }
            
            # 使用upsert更新联系人数据
            self.contact_collection.update_one(
                {'username': contact[0]},
                {'$set': contact_doc},
                upsert=True
            )
        
        conn.close()
    
    def migrate_chatrooms(self, micromsg_db_path: str):
        """增量迁移群聊数据"""
        if not os.path.exists(micromsg_db_path):
            raise FileNotFoundError(f"MicroMsg数据库文件不存在: {micromsg_db_path}")
        
        conn = sqlite3.connect(micromsg_db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT ChatRoomName, RoomData FROM ChatRoom")
        chatrooms = cursor.fetchall()
        current_time = datetime.now()
        
        for chatroom in chatrooms:
            chatroom_doc = {
                'chatroom_name': chatroom[0],
                'room_data': chatroom[1],
                'last_update_time': current_time
            }
            
            # 使用upsert更新群聊数据
            self.chatroom_collection.update_one(
                {'chatroom_name': chatroom[0]},
                {'$set': chatroom_doc},
                upsert=True
            )
        
        conn.close()

def schedule_migrate_to_mongodb(msg_db_path: str, micromsg_db_path: str, mongodb_uri: str = "mongodb://localhost:27017/", 
                               database_name: str = "wechat_msg", interval_minutes: int = 5):
    """定时执行数据迁移任务
    
    Args:
        msg_db_path: MSG数据库路径
        micromsg_db_path: MicroMsg数据库路径
        mongodb_uri: MongoDB连接URI
        database_name: MongoDB数据库名称
        interval_minutes: 同步间隔（分钟）
    """
    migrator = MongoDBMigrator(mongodb_uri, database_name)
    # 获取微信信息和密钥
    info = get_wechat_info()
    if not info:
        raise Exception("获取微信信息失败")
    db_dir = info.get('filePath')
    key = info.get('key')
    print(info)
    def sync_job():
        try:
            print(f"[{datetime.now()}] 开始同步数据...")
            # 解密数据库
            print(f"[{datetime.now()}] 开始解密数据库...")
            if not decrypt_db(db_dir, key):
                raise Exception("数据库解密失败")

            # 合并数据库
            print(f"[{datetime.now()}] 开始合并数据库...")
            if not merge_databases_wrapper():
                raise Exception("数据库合并失败")
            
            # 执行数据迁移
            migrator.migrate_messages(msg_db_path)
            migrator.migrate_contacts(micromsg_db_path)
            migrator.migrate_chatrooms(micromsg_db_path)
            print(f"[{datetime.now()}] 数据同步完成")
        except Exception as e:
            print(f"[{datetime.now()}] 数据同步失败: {str(e)}")
    
    # 立即执行一次同步
    sync_job()
    
    # 设置定时任务
    schedule.every(interval_minutes).minutes.do(sync_job)
    
    # 持续运行定时任务
    while True:
        schedule.run_pending()
        time.sleep(1)

def parse_ExtraBuf(ExtraBuf: bytes):
    """
    解析微信 Contact 表中的 ExtraBuf 字段
    :param ExtraBuf: 二进制数据
    :return: 解析后的字典
    """
    if not ExtraBuf:
        return None

    # 定义字段映射关系
    buf_dict = {
        '74752C06': '性别[1男2女]', '46CF10C4': '个性签名', 'A4D9024A': '国', 'E2EAA8D1': '省', '1D025BBF': '市',
        'F917BCC0': '公司名称', '759378AD': '手机号', '4EB96D85': '企微属性', '81AE19B4': '朋友圈背景',
        '0E719F13': '备注图片', '945f3190': '备注图片2',
        'DDF32683': '0', '88E28FCE': '1', '761A1D2D': '2', '0263A0CB': '3', '0451FF12': '4', '228C66A8': '5',
        '4D6C4570': '6', '4335DFDD': '7', 'DE4CDAEB': '8', 'A72BC20A': '9', '069FED52': '10', '9B0F4299': '11',
        '3D641E22': '12', '1249822C': '13', 'B4F73ACB': '14', '0959EB92': '15', '3CF4A315': '16',
        'C9477AC60201E44CD0E8': '17', 'B7ACF0F5': '18', '57A7B5A8': '19', '695F3170': '20', 'FB083DD9': '21',
        '0240E37F': '22', '315D02A3': '23', '7DEC0BC3': '24', '16791C90': '25'
    }

    result = {}
    for buf_name in buf_dict:
        rdata_name = buf_dict[buf_name]
        buf_name = bytes.fromhex(buf_name)  # 将十六进制字符串转换为字节
        offset = ExtraBuf.find(buf_name)  # 查找标识符的起始位置

        if offset == -1:
            result[rdata_name] = ""
            continue

        offset += len(buf_name)  # 跳过标识符
        type_id = ExtraBuf[offset: offset + 1]  # 获取数据类型标识
        offset += 1

        if type_id == b"\x04":  # 整数类型
            result[rdata_name] = int.from_bytes(ExtraBuf[offset: offset + 4], "little")
        elif type_id == b"\x18":  # UTF-16 编码字符串
            length = int.from_bytes(ExtraBuf[offset: offset + 4], "little")
            result[rdata_name] = ExtraBuf[offset + 4: offset + 4 + length].decode("utf-16").rstrip("\x00")
        elif type_id == b"\x17":  # UTF-8 编码字符串
            length = int.from_bytes(ExtraBuf[offset: offset + 4], "little")
            result[rdata_name] = ExtraBuf[offset + 4: offset + 4 + length].decode("utf-8", errors="ignore").rstrip("\x00")
        elif type_id == b"\x05":  # 十六进制数据
            result[rdata_name] = f"0x{ExtraBuf[offset: offset + 8].hex()}"
    return result

def migrate_to_mongodb(msg_db_path: str, micromsg_db_path: str, mongodb_uri: str = "mongodb://localhost:27017/", database_name: str = "wechat_msg"):
    """迁移数据到MongoDB
    
    Args:
        msg_db_path: MSG数据库路径
        micromsg_db_path: MicroMsg数据库路径
        mongodb_uri: MongoDB连接URI
        database_name: MongoDB数据库名称
    """
    try:
        # 初始化MongoDB迁移器
        migrator = MongoDBMigrator(mongodb_uri, database_name)
        
        # 执行数据迁移
        print(f"[{datetime.now()}] 开始迁移消息数据...")
        migrator.migrate_messages(msg_db_path)
        
        print(f"[{datetime.now()}] 开始迁移联系人数据...")
        migrator.migrate_contacts(micromsg_db_path)
        
        print(f"[{datetime.now()}] 开始迁移群聊数据...")
        migrator.migrate_chatrooms(micromsg_db_path)
        
        print(f"[{datetime.now()}] 数据迁移完成")
        return True
    except Exception as e:
        print(f"[{datetime.now()}] 数据迁移失败: {str(e)}")
        return False

if __name__ == "__main__":
    # 示例用法
    MSG_DB_PATH = "app/Database/Msg/MSG.db"
    MICROMSG_DB_PATH = "app/Database/Msg/MicroMsg.db"
    
    # 启动定时同步任务，每5分钟同步一次
    schedule_migrate_to_mongodb(
        msg_db_path=MSG_DB_PATH,
        micromsg_db_path=MICROMSG_DB_PATH,
        mongodb_uri="mongodb://localhost:27017/",
        database_name="t",
        interval_minutes=5
    )
