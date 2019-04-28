class ExecuteException(TypeError):
    """
        The execute parm neither 'sql' nor 'entity'
        need check parm type
    """


class SQLError(Exception):
    """
        sql build error
    """

class EntityError(Exception):
    """
        entity build error
    """

