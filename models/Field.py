import calendar
import time
import datetime
import arrow
from ..Content import Content
from ..util.SqlBuilder import ColumnBase
from Flows.Flow import rules


class FieldAccessor(object):
    def __init__(self, model, field, name):
        self.model = model
        self.field = field
        self.name = name

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            now_data = instance.__data__.get(self.name)
            if now_data is not None:
                return now_data
            else:  # return default data
                default_data = self.field.default
                if default_data is not None:
                    # return ''
                    return self.field.coerce(default_data)
            return None
        return self.field

    def __set__(self, instance, value):
        instance.__data__[self.name] = value
        instance._dirty.add(self.name)


class Field(ColumnBase):
    _field_counter = 0
    _order = 0
    field_flow = Content.flows.get(Content.FIELD_FLOW)
    accessor_class = FieldAccessor
    auto_increment = False
    field_type = 'DEFAULT'

    def __init__(self, null=False, index=False, unique=False, column_name=None,
                 default=None, primary_key=False, constraints=None,
                 sequence=None, collation=None, unindexed=False, choices=None,
                 help_text=None, verbose_name=None, _hidden=False, max_length=None):

        self.max_length = max_length
        self.default = self.coerce(default) if default is not None else default

        self.null = null
        self.index = index
        self.unique = unique
        self.column_name = column_name
        self.primary_key = primary_key
        self.constraints = constraints  # List of column constraints.
        self.sequence = sequence  # Name of sequence, e.g. foo_id_seq.
        self.collation = collation
        self.unindexed = unindexed
        self.choices = choices
        self.help_text = help_text
        self.verbose_name = verbose_name
        self._hidden = _hidden

        # Used internally for recovering the order in which Fields were defined
        # on the Model class.
        Field._field_counter += 1
        self._order = Field._field_counter
        self._sort_key = (self.primary_key and 1 or 2), self._order

        self.__data__ = dict()

    def __hash__(self):
        return hash(self.name + '.' + self.model.__name__)

    def bind(self, model, name, set_attribute=True):
        self.model = model
        self.name = name
        self.column_name = self.column_name or name
        if set_attribute:
            setattr(model, name, self.accessor_class(model, self, name))

    def coerce(self, value):
        return value

    @rules(field_flow)
    def length_check(self, val):
        bval = str(val)
        vlen = len(bval)

        if vlen > self.max_length:
            raise Exception("Value[%s] too long:[%s] -- max length:[%s]  value:[%s]"
                            % (self.column_name, len(bval), self.max_length, val))

    def checked(self, val):
        result = self.length_check(val)
        if result is not None: return self.coerce(result)
        else: return val

    def validate_self(self, val):
        try:
            if val is None and self.primary_key:
                return "__pk__"

            if val is None and self.null is True:
                return self.default
            if val is None and self.null is False and self.default is not None:
                return self.default
            elif val is None and self.null is False:
                raise TypeError("[%s] value can't be null" % self.column_name)

            cval = self.coerce(val)
            final_result = self.checked(cval)
            return final_result
        except Exception, e:
            raise e

    def db_value(self, value):
        return value if value is None else self.coerce(value)

    def python_value(self, value):
        return value if value is None else self.coerce(value)

    def asc(self):
        return "%s ASC" % self.column_name
    __pos__ = asc

    def desc(self):
        return "%s DESC" % self.column_name
    __neg__ = desc

    def __sql__(self, ctx):
        if ctx.model_alias and ctx.model_alias.has_key(self.model):
            as_obj = ctx.model_alias[self.model]
            return ctx.sql("%s.`%s`" % (as_obj.name, self.column_name))
        return ctx.sql("`%s`" % self.column_name)


class BooleanField(Field):
    field_type = 'BOOL'

    def coerce(self, value):
        if value is True:
            return 0
        elif value is False:
            return 1
        else:
            return None


class IntegerField(Field):
    field_type = 'INT'
    coerce = int


class BigIntegerField(IntegerField):
    field_type = 'BIGINT'


class AutoField(IntegerField):
    auto_increment = True
    field_type = 'AUTO'

    def __init__(self, *args, **kwargs):
        if kwargs.get('primary_key') is False:
            raise ValueError('AutoField must always be a primary key.')

        kwargs['primary_key'] = True
        super(AutoField, self).__init__(*args, **kwargs)


class FloatField(Field):
    field_type = 'FLOAT'
    coerce = float

    def __init__(self, *args, **kwargs):
        self.scale_length = kwargs.get("scale_length", 2)
        super(FloatField, self).__init__(*args, **kwargs)

    def length_check(self, val):
        bval = str(val)
        vlen = len(bval)

        if vlen > self.max_length:
            v = bval.split(".")
            scale_len, len_val = len(v[1]), ""

            if scale_len > self.scale_length:
                v[1] = v[1][0: self.scale_length]
                return ".".join(v)
            else:
                raise Exception("Value[%s] too long:[%s] -- max length:[%s]"
                                % (self.column_name, len(bval), self.max_length))


class CharField(Field):
    field_type = 'CHAR'
    coerce = str

    # def coerce(self, value):
    #     v = str(value)
    #     if "\\U" in v:
    #         return v
    #     return v.encode("unicode_escape")


class VarCharField(Field):
    field_type = 'VARCHAR'
    coerce = str

    # def coerce(self, value):
    #     v = str(value)
    #     if "\\U" in v:
    #         return v
    #     return v.encode("unicode_escape")


class TextField(Field):
    field_type = 'TEXT'
    coerce = str


class LongTextField(Field):
    field_type = 'LONGTEXT'
    coerce = str


class _BaseFormattedField(Field):
    formats = {
        "DATETIME": '%Y-%m-%d %H:%M:%S',
        "DATE": '%Y-%m-%d',
        "TIME": '%H:%M:%S.%f'
    }

    def __init__(self, formats=None, *args, **kwargs):
        if formats is not None:
            self.formats = formats
        super(_BaseFormattedField, self).__init__(*args, **kwargs)


class DateTimeField(_BaseFormattedField):
    field_type = 'DATETIME'

    def coerce(self, value):
        return arrow.get(value).strftime(self.formats[self.field_type])


class DateField(_BaseFormattedField):
    field_type = 'DATE'

    def coerce(self, value):
        if isinstance(value, datetime.date):
            return str(value)
        return arrow.get(value).strftime(self.formats[self.field_type])


class TimeField(_BaseFormattedField):
    field_type = 'TIME'

    def coerce(self, value):
        if isinstance(value, datetime.time):
            return str(value)
        return arrow.get(value).strftime(self.formats[self.field_type])


class TimestampField(IntegerField):
    # Support second -> microsecond resolution.
    valid_resolutions = [10**i for i in range(7)]

    def __init__(self, *args, **kwargs):
        self.resolution = kwargs.pop('resolution', 1) or 1
        if self.resolution not in self.valid_resolutions:
            raise ValueError('TimestampField resolution must be one of: %s' %
                             ', '.join(str(i) for i in self.valid_resolutions))

        self.utc = kwargs.pop('utc', False) or False
        _dt = datetime.datetime
        self._conv = _dt.utcfromtimestamp if self.utc else _dt.fromtimestamp
        _default = _dt.utcnow if self.utc else _dt.now
        kwargs.setdefault('default', _default)
        super(TimestampField, self).__init__(*args, **kwargs)

    def db_value(self, value):
        if value is None:
            return

        if isinstance(value, datetime.datetime):
            pass
        elif isinstance(value, datetime.date):
            value = datetime.datetime(value.year, value.month, value.day)
        else:
            return int(round(value * self.resolution))

        if self.utc:
            timestamp = calendar.timegm(value.utctimetuple())
        else:
            timestamp = time.mktime(value.timetuple())
        timestamp += (value.microsecond * .000001)
        if self.resolution > 1:
            timestamp *= self.resolution
        return int(round(timestamp))

    def python_value(self, value):
        if value is not None and isinstance(value, (int, float, long)):
            if value == 0:
                return
            elif self.resolution > 1:
                ticks_to_microsecond = 1000000 // self.resolution
                value, ticks = divmod(value, self.resolution)
                microseconds = ticks * ticks_to_microsecond
                return self._conv(value).replace(microsecond=microseconds)
            else:
                return self._conv(value)
        return value


COLUMN_SIGN = {
    "number": ["int", "bigint", "tinyint", "bit"]
    , "float": ["float", "decimal"]
    , "char": ["varchar", "char", "text", "longtext"]
    , "datetime": ["date", "datetime", "time", "timestamp"]
}


CHECK_LIST = {
    "AUTO": AutoField
    , "char": CharField
    , "varchar": VarCharField
    , "text": TextField
    , "longtext": LongTextField
    , "int": IntegerField
    , "bit": IntegerField
    , "float": FloatField
    , "decimal": FloatField
    , "bigint": BigIntegerField
    , "tinyint": BooleanField
    , "bool": BooleanField
    , "datetime": DateTimeField
    , "date": DateField
    , "time": TimeField
    , "timestamp": DateTimeField
}


# For my lazy
LITTLE_ESCAPGE = {
    "tinyint": "BOOL",
    "bit": "INT",
    "decimal": "FLOAT",
    "timestamp": "DATETIME",
}


__all__ = ["AutoField", "BigIntegerField", "BooleanField", "CharField", "VarCharField",
           "DateField", "TimeField", "DateTimeField", "TimestampField", "FloatField",
           "TextField", "LongTextField", "IntegerField", "COLUMN_SIGN", "CHECK_LIST",
           "LITTLE_ESCAPGE"]
