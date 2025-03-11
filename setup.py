from setuptools import setup
from setuptools import find_packages
from cx_Freeze import setup, Executable
import sys

build_exe_options = {
    "packages": ["os", "tkinter", "pymem"],
    "includes": ["tkinter", "tkinter.ttk"],
    "include_files": [],
    "excludes": ["matplotlib", "numpy", "PIL"],
}

base = None
if sys.platform == "win32":
    base = "Win32GUI"

setup(
    name="WeChatKeyGetter",
    version="1.0",
    description="微信密钥获取工具",
    options={"build_exe": build_exe_options},
    executables=[Executable(
        "get_key.py",
        base=base,
        target_name="WeChatKeyGetter.exe",
        icon=None
    )]
)

# `python setup.py build` 即可生成可执行文件