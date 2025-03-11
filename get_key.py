import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox
from pymem import Pymem
from app.decrypt.get_wx_info import Wechat

class WeChatKeyGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title('微信密钥获取工具')
        self.root.geometry('400x200')
        self.root.resizable(False, False)
        
        # 创建主框架
        self.main_frame = ttk.Frame(self.root, padding='10')
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 创建并配置组件
        self.status_label = ttk.Label(self.main_frame, text='点击按钮获取微信密钥')
        self.status_label.grid(row=0, column=0, columnspan=2, pady=10)
        
        self.key_text = tk.Text(self.main_frame, height=3, width=40)
        self.key_text.grid(row=1, column=0, columnspan=2, pady=10)
        self.key_text.config(state='disabled')
        
        self.get_key_button = ttk.Button(self.main_frame, text='获取密钥', command=self.get_key)
        self.get_key_button.grid(row=2, column=0, pady=10)
        
        self.copy_button = ttk.Button(self.main_frame, text='复制密钥', command=self.copy_key)
        self.copy_button.grid(row=2, column=1, pady=10)
        
        # 配置列权重
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.columnconfigure(1, weight=1)
    
    def get_key(self):
        try:
            self.status_label.config(text='正在获取密钥...')
            self.root.update()
            
            # 获取密钥
            wechat = Pymem('WeChat.exe')
            key = Wechat(wechat).GetInfo()
            
            if key:
                self.key_text.config(state='normal')
                self.key_text.delete(1.0, tk.END)
                self.key_text.insert(tk.END, key)
                self.key_text.config(state='disabled')
                self.status_label.config(text='密钥获取成功！')
            else:
                messagebox.showerror('错误', '获取密钥失败，请确保微信已登录')
                self.status_label.config(text='获取失败，请重试')
        except Exception as e:
            messagebox.showerror('错误', f'获取密钥时出错：{str(e)}')
            self.status_label.config(text='获取失败，请重试')
    
    def copy_key(self):
        key = self.key_text.get(1.0, tk.END).strip()
        if key:
            self.root.clipboard_clear()
            self.root.clipboard_append(key)
            self.status_label.config(text='密钥已复制到剪贴板')
        else:
            messagebox.showwarning('提示', '请先获取密钥')
    
    def run(self):
        self.root.mainloop()

def main():
    app = WeChatKeyGUI()
    app.run()

if __name__ == '__main__':
    main()