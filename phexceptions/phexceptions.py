# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class pharbers command context,
"""


class PhException(RuntimeError):
    """The Pharbers Exceptions

        Args:
            code: the number of exceptions
            msg: the message of exceptions

    """

    def __init__(self, code, msg):
        self.code = code
        self.msg = msg


exception_file_already_exist = PhException(-1, "the dict already exists")
exception_file_not_exist = PhException(-2, "the dict is not exists")
exception_function_not_implement = PhException(-3, "the function is not implement, please contact alfredyang@pharbers.com")
