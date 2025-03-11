import sqlite3
from datetime import datetime
import os
import sys
from typing import List, Tuple, Dict
from collections import defaultdict

# 将项目根目录添加到 sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from app.util.protocbuf import MessageBytesExtra

class UserMessageViewer:
    def __init__(self, db_path: str = "./app/Database/Msg/MSG.db", contact_db_path: str = "./app/Database/Msg/MicroMsg.db"):
        self.db_path = db_path
        self.contact_db_path = contact_db_path
        self.conn = None
        self.cursor = None
        self.contact_conn = None
        self.contact_cursor = None
        
        # 自动连接数据库
        if not os.path.exists(self.db_path) or not os.path.exists(self.contact_db_path):
            print(f"错误：数据库文件不存在: {self.db_path} 或 {self.contact_db_path}")
            return
            
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            
            self.contact_conn = sqlite3.connect(self.contact_db_path)
            self.contact_cursor = self.contact_conn.cursor()
        except sqlite3.Error as e:
            print(f"连接数据库失败：{e}")
            return
    
    def get_all_users(self) -> List[str]:
        """获取所有聊天用户列表"""
        if not self.cursor:
            return []
            
        sql = '''
            SELECT DISTINCT StrTalker
            FROM MSG
            ORDER BY CreateTime DESC
        '''
        
        try:
            self.cursor.execute(sql)
            users = [row[0] for row in self.cursor.fetchall()]
            return users
        except sqlite3.Error as e:
            print(f"查询用户列表失败：{e}")
            return []
    
    def get_user_messages(self, user_id: str, limit: int = None) -> List[Tuple]:
        """获取指定用户的聊天记录"""
        if not self.cursor:
            return []
            
        if limit is not None:
            sql = '''
                SELECT 
                    strftime('%Y-%m-%d %H:%M:%S', CreateTime, 'unixepoch', 'localtime') as time,
                    StrTalker as contact,
                    IsSender as is_sender,
                    Type as msg_type,
                    StrContent as content,
                    BytesExtra,
                    SubType,
                    CreateTime,
                    MsgSvrID,
                    CompressContent
                FROM MSG
                WHERE StrTalker = ?
                ORDER BY CreateTime ASC
                LIMIT ?
            '''
            params = (user_id, limit)
        else:
            sql = '''
                SELECT 
                    strftime('%Y-%m-%d %H:%M:%S', CreateTime, 'unixepoch', 'localtime') as time,
                    StrTalker as contact,
                    IsSender as is_sender,
                    Type as msg_type,
                    StrContent as content,
                    BytesExtra,
                    SubType,
                    CreateTime,
                    MsgSvrID,
                    CompressContent
                FROM MSG
                WHERE StrTalker = ?
                ORDER BY CreateTime ASC
            '''
            params = (user_id,)
        
        try:
            self.cursor.execute(sql, params)
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"查询用户 {user_id} 的聊天记录失败：{e}")
            return []
    
    def format_message(self, message: Tuple) -> str:
        """格式化消息显示"""
        time, contact, is_sender, msg_type, content, bytes_extra = message
        is_group = contact.endswith('@chatroom')
        
        if is_group:
            if not hasattr(self, '_last_group') or self._last_group != contact:
                self._last_group = contact
                group_name = self.get_contact_name(contact)
                prefix = f"群聊: {group_name}\n"
            else:
                prefix = ""
            
            if is_sender:
                sender = "我"
            else:
                # 初始化wxid变量
                wxid = ''
                # 解析BytesExtra获取发送者wxid
                if bytes_extra is None:  # BytesExtra为空，可能是系统消息
                    sender = "系统消息"
                else:
                    try:
                        msgbytes = MessageBytesExtra()
                        msgbytes.ParseFromString(bytes_extra)
                        for tmp in msgbytes.message2:
                            if tmp.field1 == 1:
                                wxid = tmp.field2
                                break
                        
                        if wxid:
                            # 处理带冒号的wxid格式
                            if ':' in wxid:
                                wxid = wxid.split(':')[0]
                            # 获取发送者信息
                            sql = '''
                                SELECT Remark, NickName
                                FROM Contact
                                WHERE UserName = ?
                            '''
                            self.contact_cursor.execute(sql, (wxid,))
                            result = self.contact_cursor.fetchone()
                            
                            if result:
                                sender = result[0] if result[0] else result[1]  # 优先使用备注名，没有则使用昵称
                            else:
                                sender = wxid
                        else:
                            sender = "系统消息"
                    except Exception as e:
                        sender = wxid if wxid else "未知用户"
        else:
            prefix = ""
            sender = "我" if is_sender else self.get_contact_name(contact)
        
        if msg_type == 1:  # 文本消息
            return f"{prefix}[{time}] {sender}: {content}"
        elif msg_type == 3:  # 图片
            return f"{prefix}[{time}] {sender}: [图片]"
        elif msg_type == 34:  # 语音消息显示"""
            return f"{prefix}[{time}] {sender}: [语音消息]"
        elif msg_type == 43:  # 视频
            return f"{prefix}[{time}] {sender}: [视频]"
        elif msg_type == 47:  # 表情包
            return f"{prefix}[{time}] {sender}: [表情包]"
        elif msg_type == 4903:  # 音乐与音频
            return f"{prefix}[{time}] {sender}: [音乐与音频]"
        elif msg_type == 4906:  # 文件
            return f"{prefix}[{time}] {sender}: [文件]"
        elif msg_type == 4905:  # 分享卡片
            return f"{prefix}[{time}] {sender}: [分享卡片]"
        elif msg_type == 49:  # 各类分享和转账
            if bytes_extra is None:
                return f"{prefix}[{time}] {sender}: [分享内容]"
            try:
                msgbytes = MessageBytesExtra()
                msgbytes.ParseFromString(bytes_extra)
                # 先检查是否为转账消息
                is_transfer = False
                transfer_detail = {}
                for tmp in msgbytes.message2:
                    if tmp.field1 == 5 and tmp.field2 == "2000":
                        is_transfer = True
                    elif tmp.field1 == 1:
                        transfer_detail['feedesc'] = tmp.field2
                    elif tmp.field1 == 2:
                        transfer_detail['pay_memo'] = tmp.field2
                    elif tmp.field1 == 3:
                        transfer_detail['paysubtype'] = int(tmp.field2)
                
                if is_transfer:
                    text_info_map = {
                        1: f"发起转账 {transfer_detail.get('feedesc', '')} {transfer_detail.get('pay_memo', '')}",
                        3: f"已收款 {transfer_detail.get('feedesc', '')}",
                        4: f"已退还 {transfer_detail.get('feedesc', '')}",
                        5: f"非实时转账收款 {transfer_detail.get('feedesc', '')}",
                        7: f"发起非实时转账 {transfer_detail.get('feedesc', '')}",
                    }
                    status = text_info_map.get(transfer_detail.get('paysubtype'), '[转账消息]')
                    return f"{prefix}[{time}] {sender}: {status}"
                
                # 如果不是转账消息，则按照分享内容处理
                share_type = None
                share_title = None
                for tmp in msgbytes.message2:
                    if tmp.field1 == 5:
                        share_type = tmp.field2
                    elif tmp.field1 == 1:
                        share_title = tmp.field2
                        break
                
                type_info_map = {
                    "8": "[直播分享]",
                    "33": "[小程序]",
                    "36": "[小程序]",
                    "4": "[链接]",
                    "5": "[链接]"
                }
                type_text = type_info_map.get(share_type, "[分享内容]")
                if share_title:
                    return f"{prefix}[{time}] {sender}: {type_text} {share_title}"
                return f"{prefix}[{time}] {sender}: {type_text}"
            except Exception as e:
                return f"{prefix}[{time}] {sender}: [分享内容]"
        elif msg_type == 50:  # 音视频通话
            return f"{prefix}[{time}] {sender}: [音视频通话]"
        elif msg_type == 10000:  # 系统消息
            return f"{prefix}[{time}] {sender}: [系统消息]"
        else:
            return f"{prefix}[{time}] {sender}: [未知类型消息]"
    
    def display_user_messages(self, user_id: str = None, limit: int = 20):
        """显示用户的聊天记录"""
        if user_id:
            # 显示单个用户的聊天记录
            messages = self.get_user_messages(user_id, limit)
            if not messages:
                print(f"\n未找到与用户 {user_id} 的聊天记录")
                return
                
            print(f"\n=== 与 {user_id} 的聊天记录 ===")
            for message in messages:
                print(self.format_message(message))
        else:
            # 显示所有用户的最近聊天记录
            users = self.get_all_users()
            if not users:
                print("\n未找到任何聊天记录")
                return
                
            for user in users:
                messages = self.get_user_messages(user, 5)  # 每个用户显示最近5条
                if messages:
                    print(f"\n=== 与 {user} 的聊天记录 ===")
                    for message in messages:
                        print(self.format_message(message))
                    print("-" * 50)
    
    def get_contact_name(self, wxid: str) -> str:
        """获取联系人昵称或备注名"""
        if not self.contact_cursor:
            return wxid
            
        try:
            # 检查是否为群聊
            is_group = wxid.endswith('@chatroom')
            
            sql = '''
                SELECT Remark, NickName
                FROM Contact
                WHERE UserName = ?
            '''
            self.contact_cursor.execute(sql, (wxid,))
            result = self.contact_cursor.fetchone()
            
            if result:
                name = result[0] if result[0] else result[1]  # 优先使用备注名，没有则使用昵称
                if is_group:
                    return f"群聊: {name}"
                return name
            return wxid
        except sqlite3.Error as e:
            print(f"查询联系人信息失败：{e}")
            return wxid
            
    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        if self.contact_cursor:
            self.contact_cursor.close()
        if self.contact_conn:
            self.contact_conn.close()

    def export_messages_to_txt(self, user_id: str = None, limit: int = None, output_dir: str = "./output"):
        """导出聊天记录到txt文件"""
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        if user_id:
            # 导出单个用户的聊天记录
            messages = self.get_user_messages(user_id, limit)
            if not messages:
                print(f"\n未找到与用户 {user_id} 的聊天记录")
                return
                
            # 获取用户昵称作为文件名
            filename = self.get_contact_name(user_id).replace(':', '_')
            filepath = os.path.join(output_dir, f"{filename}.txt")
            
            with open(filepath, 'w', encoding='utf-8') as f:
                title = f"=== 与 {filename}{' (' + user_id + ')' if user_id.endswith('@chatroom') else ''} 的聊天记录 ===\n"
                f.write(title)
                for message in messages:
                    f.write(self.format_message(message) + '\n')
            print(f"聊天记录已导出到: {filepath}")
        else:
            # 导出所有用户的聊天记录
            users = self.get_all_users()
            if not users:
                print("\n未找到任何聊天记录")
                return
                
            for user in users:
                messages = self.get_user_messages(user, limit)
                if messages:
                    # 获取用户昵称作为文件名
                    filename = self.get_contact_name(user).replace(':', '_')
                    filepath = os.path.join(output_dir, f"{filename}.txt")
                    
                    with open(filepath, 'w', encoding='utf-8') as f:
                        title = f"=== 与 {filename}{' (' + user + ')' if user.endswith('@chatroom') else ''} 的聊天记录 ===\n"
                        f.write(title)
                        for message in messages:
                            f.write(self.format_message(message) + '\n')
                    print(f"聊天记录已导出到: {filepath}")

    def get_user_all_messages(self, target_wxid: str, limit: int = None) -> List[Tuple]:
        """获取与指定用户相关的所有聊天记录，包括群聊中的消息"""
        if not self.cursor:
            return []
            
        if limit is not None:
            sql = '''
                SELECT 
                    strftime('%Y-%m-%d %H:%M:%S', CreateTime, 'unixepoch', 'localtime') as time,
                    StrTalker as contact,
                    IsSender as is_sender,
                    Type as msg_type,
                    StrContent as content,
                    BytesExtra,
                    SubType,
                    CreateTime,
                    MsgSvrID,
                    CompressContent
                FROM MSG
                WHERE (StrTalker = ? OR 
                    (StrTalker LIKE '%@chatroom' AND BytesExtra IS NOT NULL AND BytesExtra LIKE ?))
                    AND Type = 1
                ORDER BY CreateTime ASC
                LIMIT ?
            '''
            params = (target_wxid, f'%{target_wxid}%', limit)
        else:
            sql = '''
                SELECT 
                    strftime('%Y-%m-%d %H:%M:%S', CreateTime, 'unixepoch', 'localtime') as time,
                    StrTalker as contact,
                    IsSender as is_sender,
                    Type as msg_type,
                    StrContent as content,
                    BytesExtra,
                    SubType,
                    CreateTime,
                    MsgSvrID,
                    CompressContent
                FROM MSG
                WHERE (StrTalker = ? OR 
                    (StrTalker LIKE '%@chatroom' AND BytesExtra IS NOT NULL AND BytesExtra LIKE ?))
                    AND Type = 1
                ORDER BY CreateTime ASC
            '''
            params = (target_wxid, f'%{target_wxid}%')
        
        try:
            self.cursor.execute(sql, params)
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"查询用户 {target_wxid} 的聊天记录失败：{e}")
            return []

def main():
    # 使用相对路径或绝对路径指定数据库文件位置
    # viewer = UserMessageViewer("./re/db/MSG.db", "./re/db/MicroMsg.db")
    viewer = UserMessageViewer()
    
    if not viewer.connect():
        return
    try:
        while True:
            print("\n请选择操作：")
            print("1. 显示所有用户的聊天记录")
            print("2. 显示指定用户的聊天记录")
            print("3. 导出所有聊天记录到文件")
            print("4. 导出指定群聊记录到文件")
            print("5. 退出")
            
            choice = input("请输入选项")
            
            if choice == "1":
                viewer.display_user_messages()
            elif choice == "2":
                user_id = input("请输入用户ID：")
                limit = int(input("请输入显示的消息数量（默认20）：") or "20")
                viewer.display_user_messages(user_id, limit)
            elif choice == "3":
                viewer.export_messages_to_txt()
            elif choice == "4":
                group_id = input("请输入群聊ID：")
                if not group_id.endswith('@chatroom'):
                    print("错误：输入的不是有效的群聊ID，群聊ID应以@chatroom结尾")
                else:
                    viewer.export_messages_to_txt(group_id)
            elif choice == "5":
                break
            else:
                print("无效的选项，请重新选择")
    
    finally:
        viewer.close()

if __name__ == "__main__":
    main()