from .micro_msg import MicroMsg
from .media_msg import MediaMsg
from .misc import Misc
from .msg import Msg
from .msg import MsgType
from .hard_link import HardLink

misc_db = Misc()
msg_db = Msg()
micro_msg_db = MicroMsg()
hard_link_db = HardLink()
media_msg_db = MediaMsg()


def close_db():
    misc_db.close()
    msg_db.close()
    micro_msg_db.close()
    hard_link_db.close()
    media_msg_db.close()


def init_db():
    misc_db.init_database()
    msg_db.init_database()
    micro_msg_db.init_database()
    hard_link_db.init_database()
    media_msg_db.init_database()


__all__ = ['misc_db', 'micro_msg_db', 'msg_db', 'hard_link_db', 'MsgType', "media_msg_db", "close_db"]
