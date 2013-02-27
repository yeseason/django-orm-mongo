import pymongo
from pymongo.cursor import Cursor
from django.db.models import signals
import datetime
from admin import settings
from importlib import import_module

class Field(object):
    def __init__(self, name=None, default=None):
        self.name = name
        self.default = default
        
    def to_value(self, value):
        return value
    
    def get_default(self):
        return self.default
    
class ForeignRelated(object):
    def __init__(self, rel, name, rel_field_name):
        self.rel = rel
        self.name = name
        self.cache_name = '_cache_%s' % self.name
        self.rel_field_name = rel_field_name
        
    def __get__(self, instance, owner):
        if instance is None:
            return self
        try:
            return getattr(instance, self.cache_name)
        except:
            spec = {self.rel_field_name:instance}
            rel_objs = self.rel.objects.filter(**spec)
            setattr(instance, self.cache_name, rel_objs)
            return rel_objs

class ForeignKey(object):
    def __init__(self, rel, name=None, default=None, related_name=None, null=False):
#        if isinstance(rel, str):
#            self.rel = rel
#            self.field_name = rel.lower()
#            #raise Exception
#        elif issubclass(rel, Document):
#            self.rel = rel
#            self.field_name = rel.__name__.lower()
#        else:
#            raise TypeError('%s not Document Class' % rel)
        self.rel = rel
        self.name = name
        self.default = None
#        self.cache_name = '_cache_%s' % self.field_name
#        self.rel_key = self.name or '%s_id' % self.field_name
        self.related_name = related_name
        self.null = null

    def to_value(self, value):
        obj = self.rel.objects.get(id=value)
        return obj
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        try:
            return getattr(instance, self.cache_name)
        except:
            rel_id = getattr(instance, self.rel_key, None)
            if rel_id is None:
                return None
            rel_obj = self.to_value(rel_id)
            setattr(instance, self.cache_name, rel_obj)
            return rel_obj
    
    def __set__(self, instance, value):
        if instance is None:
            raise Exception('%s must be accessed via instance' % self.rel._meta.object_name)
        if value is None:
            if self.null:
                setattr(instance, self.cache_name, value)
                setattr(instance, self.rel_key, 0)
            else:
                raise Exception('%s must not None' % self.rel._meta.object_name)
        else:
            if not isinstance(value, self.rel):
                raise Exception('%s class type error' % self.rel._meta.object_name)
            setattr(instance, self.cache_name, value)
            setattr(instance, self.rel_key, value.pk)
        
class GenerForeignKey(object):
    def __init__(self, type_field="content_type", pk_field="object_id"):
        self.type_field = type_field
        self.pk_field = pk_field
        
    def set_name(self, name):
        self.name = name
        self.cache_name = '_cache_%s' % self.name
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        try:
            return getattr(instance, self.cache_name)
        except:
            type = getattr(instance, self.type_field)
            pk = getattr(instance, self.pk_field)
            mn, cls_name = type.split('.', 1)
            tmcls = instance.__class__
            modulename = tmcls.__module__
            mname = '%s.%s' % (modulename.rsplit('.', 1)[0], mn)
            module = import_module[mname]
            cls = getattr(module, cls_name)
            obj = cls.objects.get(pk=pk)
            return obj
        
    def __set__(self, instance, value):
        if instance is None:
            raise Exception('%s must be accessed via instance' % self.rel._meta.object_name)
        if not isinstance(value, Document):
            raise Exception('%s class type error' % self.rel._meta.object_name)
        setattr(instance, self.cache_name, value)
        cls = value.__class__
        mname = cls.__module__.rsplit('.', 1)[1]
        type = '%s.%s' % (mname, cls.__name__)
        pk = value.pk
        setattr(instance, self.type_field, type)
        setattr(instance, self.pk_field, pk)
    
class IntegerField(Field):
    def __init__(self, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = 0
        super(IntegerField, self).__init__(**kwargs)
        
    def to_value(self, value):
        try:
            return int(value)
        except:
            return 0
    
    def get_default(self):
        return self.default
    
class BooleanField(Field):
    def __init__(self, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = False
        super(BooleanField, self).__init__(**kwargs)
        
    def to_value(self, value):
        return bool(value)
    
    def get_default(self):
        return self.default or False
    
class DatetimeField(Field):
    def __init__(self, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = datetime.datetime.now()
        super(DatetimeField, self).__init__(**kwargs)
        
    def to_value(self, value):
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        if isinstance(value, unicode):
            value = str(value)
        if isinstance(value, str):
            return parse_datetime(value)
    
class DateField(Field):
    def __init__(self, **kwargs):
        if 'default' not in kwargs:
            today = datetime.date.today()
            dt = datetime.datetime(today.year, today.month, today.day)
            kwargs['default'] = dt
        super(DateField, self).__init__(**kwargs)
        
    def to_value(self, value):
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        if isinstance(value, unicode):
            value = str(value)
        if isinstance(value, str):
            return parse_datetime(value)
        
class ArrayField(Field):
    def __init__(self, **kwargs):
        if 'sep' in kwargs:
            self.sep = kwargs['sep']
        else:
            self.sep = ' '
        super(ArrayField, self).__init__(**kwargs)
        
    def to_value(self, value):
        if isinstance(value, str):
            return value.split(self.sep)
        if isinstance(value, unicode):
            return value.split(self.sep)
        return value
        
    def get_default(self):
        return self.default

class MCursor(Cursor):
    def __init__(self, cursor, document):
        self.__dict__ = cursor.__dict__
        self.document = document
    
    def set_document(self, document):
        self.document = document
    
    def __getitem__(self, index):
        item = super(MCursor, self).__getitem__(index)
        if isinstance(item, Cursor):
            return list(item)
        return self._data2obj(item)
    
    def next(self):
        item = super(MCursor, self).next()
        return self._data2obj(item)
    
    def _data2obj(self, data):
        if not data:
            return None
        obj = self.document(**data)
        if getattr(obj, 'id', None) is None:
            name = self.document._meta.collection_name
            obj.id = AutoID.get_id(name)
            obj.save()
        return obj
    
    def order_by(self, key):
        direction = pymongo.ASCENDING
        if key.find('-') == 0:
            key = key[1:]
            direction = pymongo.DESCENDING
        return super(MCursor, self).sort(key, direction)
    
    def __len__(self):
        return self.count()
    
    def all(self):
        return self

class Manager(object):
    opts = ('in', 'nin', 'lt', 'gt', 'ne', 'lte', 'gte',)
    
    def __init__(self, document=None):
        self.document = document
        self._db = None
        self._collection = None
        
    def set_document(self, document):
        self.document = document
        
    def get_db(self):
        if not self._db:
            client = pymongo.MongoClient()
            dbname = settings.MONGODBS['default']
            self._db = client[dbname]
        return self._db
    
    def using(self, name):
        client = pymongo.MongoClient()
        self._db = client[name]
        return self

    def collection(self):
        if not self._collection:
            db = self.get_db()
            collection_name = self.document._meta.collection_name
            self._collection = db[collection_name]
        return self._collection
    
    def get(self, **kwargs):
        data = self.collection().find_one(self._clean_kwargs(kwargs))
        return self._data2obj(data)
    
    def create(self, **kwargs):
        obj = self._data2obj(kwargs)
        obj.save()
        return obj
    
    def save(self, **kwargs):
        d = self._clean_kwargs(kwargs)
        result = self.collection().save(d)
        return result
    
    def get_or_create(self, **kwargs):
        obj = self.get(**kwargs)
        created = False
        if not obj:
            obj = self.create(**kwargs)
            created = True
        return (obj, created)
    
    def filter(self, **kwargs):
        cursor = self.collection().find(self._clean_kwargs(kwargs))
        return MCursor(cursor, self.document)
    
    def update(self, spec, **kwargs):
        upset = False
        if 'upset' in kwargs:
            upset = kwargs['upset']
            del kwargs['upset']
        multi = False
        if 'multi' in kwargs:
            multi = kwargs['multi']
            del kwargs['multi']
        doc = self._parse_kwargs(kwargs)
        spec = self._clean_kwargs(spec)
        if '$inc' not in doc and '$set' not in doc:
            raise Exception('%s update doc error' % doc)
        return self.collection().update(spec, doc, upset, multi)
    
    def remove(self, **kwargs):
        spec = self._clean_kwargs(kwargs)
        if not spec:
            raise Exception('%s remove spec error' % spec)
        return self.collection().remove(spec)
    
    def delete(self, **kwargs):
        mcursor = self.filter(**kwargs)
        for obj in mcursor:
            obj.delete()
        
    def _data2obj(self, data):
        if not data:
            return None
        obj = self.document(**data)
        return obj
    
    def all(self):
        cursor = self.collection().find()
        mcursor = MCursor(cursor, self.document)
        return mcursor
    
    def count(self):
        return self.collection().count()
    
    def _parse_kwargs(self, kwargs):
        doc = {}
        inc = {}
        set = {}
        for k, v in kwargs.items():
            if isinstance(v, Document):
                t = self._parse_relate_field(k, v)
                if isinstance(t, tuple):
                    set[t[0]] = t[1]
                if isinstance(t, list):
                    for a, b in t:
                        set[a] = b
            elif k.find('__') > 0:
                tmp1, tmp2 = k.split('__', 1)
                if tmp2 == 'inc':
                    inc[tmp1] = self._parse_value(tmp1, v)
                else:
                    set[tmp1+'.'+tmp2] = v
            else:
                set[k] = self._parse_value(k, v)
        if inc:
            doc['$inc'] = inc
        if set:
            doc['$set'] = set
        return doc
        
    def _clean_kwargs(self, kwargs):
        params = []
        for k, v in kwargs.items():
            t = self._clean_arg(k, v)
            if isinstance(t, list):
                for a in t:
                    params.append(a)
            else:
                if t:
                    params.append(t)
        return dict(params)
        
    def _clean_arg(self, key, value):
        if isinstance(value, Document):
            return self._parse_relate_field(key, value)
        elif key == 'pk':
            key = 'id'
            value = int(value)
        elif key == 'id':
            value = int(value)
        elif key.find('__') > 0:
            tmp1, tmp2 = key.split('__', 1)
            if tmp2 in self.opts:
                key = tmp1
                value = {'$'+tmp2:self._parse_value(key, value)}
            else:
                value = self._parse_value(key, value)
                key = tmp1+'.'+tmp2
        elif key.find('_') == 0 and key != '_id':
            return None
        return (key, value)
    
    def _parse_relate_field(self, key, value):
        cls_dict = self.document.__dict__
        if key in cls_dict:
            field = cls_dict[key]
            if isinstance(field, ForeignKey):
                key = cls_dict[key].rel_key
                value = (value and value.pk) or 0
                return (key, value)
            elif isinstance(field, GenerForeignKey):
                vcls = value.__class__
                type = '%s.%s' % (vcls.__module__, vcls.__name__)
                pk = value.pk
                return [(field.type_field, type), (field.pk_field, pk)]
        return (key + '_id', value.pk)
    
    def _parse_value(self, key, value):
        fields = self.document._meta.fields
        if key in fields:
            return fields[key].to_value(value)
        return value

class DocumentBase(type):
    def __new__(cls, name, bases, attrs):
        super_new = super(DocumentBase, cls).__new__
        parents = [b for b in bases if isinstance(b, DocumentBase)]
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return super_new(cls, name, bases, attrs)
        # Create the class.
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})
        attr_meta = attrs.pop('Meta', None)
        abstract = getattr(attr_meta, 'abstract', False)
        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta
        # Add all attributes to the class.
        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)
#        new_class = super_new(cls, name, bases, attrs)
#        meta = getattr(new_class, 'Meta')
        new_class.set_meta_options(meta)
        for parent in [cls for cls in parents if hasattr(cls, '_meta')]:
            parent_fields = parent._meta.fields
            for name, field in parent_fields.items():
                new_class.add_to_class(name, field)
        new_class.set_objects_document()
        return new_class
    
    def add_to_class(cls, name, value):
        setattr(cls, name, value)
    
    def set_meta_options(cls, meta):
        _meta = meta()
        _meta.app_name = getattr(meta, 'app_name', None)
        if _meta.app_name is None:
            _meta.app_name = cls.__module__.split('.')[-1]
        _meta.object_name = cls.__name__
        _meta.module_name = _meta.object_name.lower()
        _meta.collection_name = getattr(meta, 'collection_name', None)
        if _meta.collection_name is None:
            _meta.collection_name = '%s_%s' % (_meta.app_name, _meta.module_name)
        #fields
        _meta.fields = {}
        cls_dict = cls.__dict__
        for k, v in cls_dict.items():
            if isinstance(v, Field):
                v.name = k
                _meta.fields[k] = v
        cls._meta = _meta
        #related_set
        for k, v in cls_dict.items():
            if isinstance(v, ForeignKey):
                vdict = v.__dict__
                rel_name = v.related_name or '%s_set' % _meta.module_name
                rel_key = v.name or '%s_id' % k
                v.rel_key = rel_key
                v.cache_name = '_cache_%s' % k
                _meta.fields[rel_key] = IntegerField()
                
                if isinstance(v.rel, Document):
                    if getattr(v.rel, rel_name, None) is None:
                        rset = ForeignRelated(cls, rel_name, k)
                        setattr(v.rel, rel_name, rset)
                    else:
                        raise Exception('%s foreign related %s is used' % (v.rel, rel_name))
                elif isinstance(v.rel, str):
                    v.rel = cls
                    rset = ForeignRelated(cls, rel_name, k)
                    setattr(cls, rel_name, rset)
                    pass
        #gener foreign key
        for k, v in cls_dict.items():
            if isinstance(v, GenerForeignKey):
                v.set_name(k)
        
    def set_objects_document(cls):
        cls.objects = Manager(cls)

class Document(object):
    __metaclass__ = DocumentBase
    
    def __init__(self, **kwargs):
        cls = self.__class__
        fields = cls._meta.fields
        for k, field in fields.items():
            setattr(self, k, field.get_default())
        self.update(**kwargs)
            
    def update(self, **kwargs):
        cls = self.__class__
        fields = cls._meta.fields
        for k, v in kwargs.items():
            if k in fields:
                v = fields[k].to_value(v)
            setattr(self, k, v)
    
    @property
    def pk(self):
        try:
            return self.id
        except:
            return None
    
    def save(self):
        origin = self.__class__
        created = not self.pk
        if created:
            self.id = self._get_autoid()
        signals.pre_save.send(sender=origin, instance=self)
        data_copy = self.__dict__.copy()
        result = self.objects.save(**data_copy)
        signals.post_save.send(sender=origin, instance=self, created=created)
        return result
        
    def delete(self):
        if not self.pk:
            return False
        origin = self.__class__
        signals.pre_delete.send(sender=origin, instance=self)
        cls_dict = self.__class__.__dict__
        for k, v in cls_dict.items():
            if isinstance(v, ForeignRelated):
                spec = {v.rel_field_name:self}
                v.rel.objects.delete(**spec)
        self.objects.remove(id=self.pk)
        signals.post_delete.send(sender=origin, instance=self)
        
    def _get_autoid(self):
        origin = self.__class__
        name = origin._meta.collection_name
        return AutoID.get_id(name)
        
    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.pk == other.pk
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    class Meta:
        pass

class AutoID(Document):
    @classmethod
    def get_id(cls, name):
        spec = {'name':name}
        cls.objects.update(spec, id__inc=1, upset=True)
        obj = cls.objects.get(name=name)
        return obj.id
    
    class Meta:
        app_name = 'mongo'
    