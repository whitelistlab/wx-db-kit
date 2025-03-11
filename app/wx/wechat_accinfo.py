import os
import sys
import re
from typing import List
from app.util import path


class UserInfo:
    def __init__(self, user_id: str, name: str, email: List[str], mobile: List[str], avatar: List[str], other: List[str]):
        self.id = user_id
        self.name = name
        self.email = email
        self.mobile = mobile
        self.avatar = avatar
        self.other = other

    def __str__(self) -> str:
        return (
            f"Id:\t{self.id}\n"
            f"Name:\t{self.name}\n"
            f"Email:\t[{', '.join(self.email)}]\n"
            f"mobile:\t[{', '.join(self.mobile)}]\n"
            f"Avatar:\t[{', '.join(self.avatar)}]\n"
            f"Other:\t[{', '.join(self.other)}]"
        )


class WeChatUserInfo:
    def __init__(self, root_path: str):
        self.root_path = root_path

    def get_user_paths(self) -> List[str]:
        """获取用户路径列表，该列表包含具有特定配置文件的目录路径"""
        user_paths = []
        try:
            for directory in os.listdir(self.root_path):
                directory_path = os.path.join(self.root_path, directory)
                if os.path.isdir(directory_path):
                    path = os.path.join(directory_path, 'config', 'AccInfo.dat')
                    if os.path.exists(path):
                        user_paths.append(path)
        except Exception as e:
            print("Error: Please enter the correct folder directory")
        return user_paths

    def query_file_info(self, text: str, search_string: str, end_string: str = "") -> str:
        """从给定的文本中查询信息，返回包含搜索字符串和结束字符串之间的内容"""
        if search_string and end_string:
            start_index = text.find(search_string)
            if start_index != -1:
                end_index = text.find(end_string, start_index + len(search_string))
                if end_index != -1:
                    return text[start_index:end_index + len(end_string)]
        elif search_string and not end_string:
            start_index = text.find(search_string)
            if start_index != -1:
                return text[start_index:]
        return ""

    def extract_invisible_characters(self, input_str: str) -> re.Pattern:
        """从输入字符串中提取不可见字符，并返回一个正则表达式模式"""
        invisible_chars = set()
        for i, char in enumerate(input_str):
            if not char.isprintable() or char.isspace():
                invisible_chars.add(char)

        pattern = f"[{''.join(map(re.escape, invisible_chars))}][^{''.join(map(re.escape, invisible_chars))}]+"
        return re.compile(pattern)

    def replace_invisible_characters(self, text: str) -> str:
        """替换文本中的不可见字符"""
        return ''.join(char if char.isprintable() and not char.isspace() else '' for char in text)

    def get_user_infos(self) -> List[UserInfo]:
        """获取用户信息列表，从包含特定格式数据的文件中提取用户信息并返回"""
        user_infos = []

        for path in self.get_user_paths():
            try:
                with open(path, 'rb') as f:
                    file_texts = f.read().decode('utf-8', errors='ignore')

                wechat_id = self.replace_invisible_characters(
                    self.query_file_info(file_texts, "\b\u0004\u0012", "\b\n\u0012")
                )

                other_data = self.query_file_info(file_texts, "\b\n\u0012")
                pattern = self.extract_invisible_characters(other_data)
                matches = [match.group() for match in pattern.finditer(other_data)]
                matches = [match.strip() for match in matches if match.strip()]

                if not matches:
                    continue

                wechat_name = matches[0]
                wechat_mobile = []
                wechat_email = []
                wechat_avatar = []
                wechat_other = []

                email_regex = re.compile(r'[a-zA-Z0-9!#$%&\'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&\'*+/=?^_`{|}~-]+)*@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9]+$')
                mobile_regex = re.compile(r'^1\d{10}$')
                url_regex = re.compile(r'^https?://([a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]\.)+[a-zA-Z0-9]+(/[a-zA-Z0-9-./?%&=]*)?$')

                for text in matches[1:]:
                    text = text.strip()
                    if email_regex.match(text):
                        wechat_email.append(text)
                    elif mobile_regex.match(text):
                        wechat_mobile.append(text)
                    elif url_regex.match(text):
                        wechat_avatar.append(text)
                    else:
                        wechat_other.append(text)

                user_infos.append(UserInfo(
                    wechat_id,
                    wechat_name,
                    wechat_email,
                    wechat_mobile,
                    wechat_avatar,
                    wechat_other
                ))

            except Exception as ex:
                print(f"File read failed: {ex}")

        return user_infos


def main():
    wx_base_dir = path.wx_path()
    wechat_info = WeChatUserInfo(wx_base_dir)
    user_infos = wechat_info.get_user_infos()
    print('\n===========================\n'.join(str(info) for info in user_infos))


if __name__ == '__main__':
    main()