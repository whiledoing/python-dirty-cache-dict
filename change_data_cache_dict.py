# coding:utf-8

'''

ChangeDataCache:  修改数据缓冲区 ，用在entity中数据的修改，入库和同步逻辑中。

操作有几个接口：

1. remove
2. update
3. pull
4. push_to_set
5. push

remove 和 update 对应 dict 的操作
pull 和 push 对应 list 的操作，如果使用 push_to_set 接口，那么加入数据后，必须当前数据没有新加入数据才会加入成功。

操作接口具有覆盖性，比如 remove 操作会覆盖之前所有的操作，操作的优先级别为：

    remove > update > pull > push_to_set > push

数据缓冲修改之后，使用 `pack_cache` 接口将修改cache打包，用于后续入库和数据同步。

@note 如果可以尽量不要使用list，如果必须使用list，最好也是简单的list类型，不要很复杂的类型。如果很复杂类型，转换为有key的dict
形式更加合理。

#-----------------------------------------------------------------------------------------------------------------------

从ChangeDataCache中得到的字典数据，会被类型为ChangeDataCacheDictItem的对象所组合，该类封装了基本dict的操作接口，使用起来和dict看起来无异，
但是在修改数据的时候，会同时调用ChangeDataCache接口跟踪数据修改。

同理，ChangeDataCacheListItem 用来实现记录修改的 List 类型

'''

import copy

class ChangeDataCacheListItem(object):
    def __init__(self, parent, k, data):
        assert isinstance(data, list), 'invalid list type'

        self._parent = parent
        self._k = k
        self._data = data

    def data(self):
        return self._data

    def __str__(self):
        return str(self._data)

    def __contains__(self, k):
        return k in self._data

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def _is_index_in_range(self, index):
        return 0 <= index < len(self._data)

    def _check_index_range(self, index):
        if not self._is_index_in_range(index):
            raise IndexError('ChangeDataCacheListItem.pop: index out of range')

    def _notify_dirty(self):
        self._parent.update_cache_data(self._k)

    def append(self, v):
        self.push(v, push_to_set=False)

    def pop(self, index):
        self._check_index_range(index)
        self._notify_dirty()
        return self._data.pop(index)

    def remove(self, k):
        if k not in self._data:
            raise ValueError('ChangeDataCacheListItem.remove: value not in list')

        self._data.remove(k)
        self._notify_dirty()

    def count(self, v):
        return self._data.count(v)

    def extend(self, v):
        self._data.extend(v)
        self._notify_dirty()

    def index(self, v, start = None, end = None):
        if start is None: start = 0
        if end is None: end = len(self._data)
        return self._data.index(v, start, end)

    # 扩展的结构，不会raise exception，支持 push to set 模式，具有返回值记录是否成功
    def push(self, v, push_to_set=False):
        if push_to_set and (v in self._data):
            return False

        self._data.append(v)
        self._notify_dirty()
        return True

    # 扩展的结构，不会raise exception，类似remove，具有返回值说明是否成功
    def pull(self, v):
        if v not in self._data:
            return False

        self._data.remove(v)
        self._notify_dirty()
        return True

    # 扩展的接口，将list替换为另外一个list
    def reset_list(self, new_list):
        self._data[:] = new_list
        self._notify_dirty()

    # 扩展接口，替换index位置元素
    def replace_at_index(self, index, v):
        if not self._is_index_in_range(index): return False

        self._data[index] = v
        self._notify_dirty()
        return True

    # 扩展接口，替换数值
    def replace_value(self, old_value, new_value):
        return self.replace_at_index(self.index(old_value), new_value) if old_value in self._data else False

class ChangeDataCacheDictItem(object):
    def __init__(self, parent, k, data):
        assert isinstance(data, dict), 'invalid dict type'

        self._parent = parent
        self._k = k
        self._data = data

    def data(self):
        return self._data

    def _get_whole_k(self, k):
        return '%s.%s' % (self._k, k)

    def __str__(self):
        return str(self._data)

    def __contains__(self, k):
        return k in self._data

    def __getitem__(self, k):
        # 如果是字典或者list类型，hook it!
        v = self._data[k]
        if isinstance(v, list):
            return ChangeDataCacheListItem(self._parent, self._get_whole_k(k), v)
        elif isinstance(v, dict):
            return ChangeDataCacheDictItem(self._parent, self._get_whole_k(k), v)
        else:
            return v

    def __setitem__(self, k, v):
        # 保证进来到_data中的数据都是普通类型数据
        if isinstance(v, ChangeDataCacheDictItem) or isinstance(v, ChangeDataCacheListItem):
            v = v._data

        self._data[k] = v
        self._parent.update_cache_data(self._get_whole_k(k))

    def __delitem__(self, k):
        del self._data[k]
        self._parent.remove_cache_data(self._get_whole_k(k))

    def __iter__(self):
        # @note key类型不会是复杂的类型，所以直接返回即可
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def setdefault(self, k, default_value = None):
        if k not in self._data:
            self.__setitem__(k, default_value)

        return self.__getitem__(k)

    def has_key(self, k):
        return self.__contains__(k)

    # @note 接口和系统的有些区别，如果找不到k，返回None
    def get(self, k, d=None):
        if k not in self._data:
            return d

        return self.__getitem__(k)

    # @note 扩展接口，得到原始数据，一般用来在不需要跟踪数据变化的情况下使用
    def get_raw(self, k, d=None):
        return self._data.get(k, d)

    # @note 接口和系统的有些区别，如果找不到k，返回None
    def pop(self, k, d=None):
        if k not in self._data:
            return d

        v = self.__getitem__(k)
        self.__delitem__(k)
        return v

    # @note update 比较特殊，只要update，是记录当前层级数据改变，而不是一个个计算子数据的改变，@TODO
    def update(self, d):
        self._data.update(d)
        self._parent.update_cache_data(self._k)

    # @note 扩展接口，只修改数据，但是不记录cache
    def update_with_no_cache(self, d):
        self._data.update(d)

    def popitem(self):
        try:
            k = next(iter(self))
        except StopIteration:
            raise KeyError

        return k, self.pop(k)

    # @note clear 同样，记录当前层级数据 dirty 即可
    def clear(self):
        self._data.clear()
        self._parent.update_cache_data(self._k)

    def keys(self):
        return self._data.keys()

    def iterkeys(self):
        return self._data.iterkeys()

    def values(self):
        return [self.__getitem__(k) for k in self._data.iterkeys()]

    def itervalues(self):
        return (self.__getitem__(k) for k in self._data.iterkeys())

    def items(self):
        return [(k, self.__getitem__(k)) for k in self._data.iterkeys()]

    def iteritems(self):
        return ((k, self.__getitem__(k)) for k in self._data.iterkeys())

d = ChangeDataCache({'jiangjinzheng': hangzhou, 'jianglei': shanghai, 'money': 100})

# @note data默认是各个层级都是字典的字典类型
def _delete_dict_recursive(data, k_list):
    if len(k_list) == 0: return True
    k = k_list[0]

    # 最后一个层级，直接删除
    if len(k_list) == 1:
        data.pop(k, None)
        return True

    # 递归删除
    if not k in data: return True

    # 到了这里，表示list还没有空，但是data空了，说明要删除的data层级比要更新的层级高，那么删除失败
    if not isinstance(data[k], dict): return False
    res = _delete_dict_recursive(data[k], k_list[1:])

    # 如果删除之后，当前层没有了数据，那么删除当前层数据
    if len(data[k]) == 0: data.pop(k)
    return res

def _set_dict_recursive(data, k_list, value=True):
    for name in k_list[0:-1]:
        # @note 如果中间由一层不是dict，说明到了最后一层，也说明之前修改层级高于当前要修改层级，不需要在记录了，因为
        # 修改层级默认记录最大一层
        data = data.setdefault(name, dict())
        if not isinstance(data, dict): return

    # True只是占位符号，表示有修改而已，实际数据在pack时候进行打包封装
    data[k_list[-1]] = value

# 递归打包update数据，pd为缓存中标记的update数据，od为原始数据，db_dict记录当前入库的数据，db_prefix记录入库数据前缀
def _pack_update_data_recursive(pd, od, db_dict, db_prefix=''):
    for k in pd.keys():
        if k not in od:
            continue

        new_prefix = db_prefix
        if new_prefix != '': new_prefix += '.'
        new_prefix += k

        if pd[k] is True:
            db_dict[new_prefix] = od[k]
        else:
            _pack_update_data_recursive(pd[k], od[k], db_dict, new_prefix)

# 递归打包删除数据
def _pack_remove_data_recursive(rd, od, db_dict, db_prefix=''):
    for k in rd.keys():
        # @note 先计算删除数据的prefix
        new_prefix = db_prefix
        if new_prefix != '': new_prefix += '.'
        new_prefix += k

        # @note 就是需要数据不存在，如果在内存被删除了，那么可能
        # 1）刚好remove cache数据也是最后一层
        # 2）用户没有使用指定接口操作，导致不一致。
        # 但一切按照内存数据为准，如果没有了，那么记录删除
        if k not in od:
            db_dict[new_prefix] = True
            continue

        # 如果出现当前数据被标记删除，但是数据还在，那么也出现数据不一致情况
        # 直接忽略，因为一切哪找内存数据为准
        if rd[k] is True:
            continue
        else:
            _pack_remove_data_recursive(rd[k], od[k], db_dict, new_prefix)

class ChangeDataCache(object):
    def __init__(self, data):

        # data 一定要是dict
        self._data = data
        self.clear_cache()
        self._can_cache = True

    def clear_cache(self):
        self._remove = {}
        self._update = {}

    def begin_cache(self):
        self._can_cache = True

    def stop_cache(self):
        self._can_cache = False

    # 打包数据，@note 不要修改打包产生的数据，不然也会同时修改原始数据
    def pack_cache(self, clear_cache=True):
        res = {}

        if len(self._remove) > 0:
            remove_db_dict = {}
            _pack_remove_data_recursive(self._remove, self._data, remove_db_dict)
            res['remove'] = remove_db_dict

        if len(self._update) > 0:
            update_db_dict = {}
            _pack_update_data_recursive(self._update, self._data, update_db_dict)
            res['update'] = update_db_dict

        # @note 因为现在逻辑该函数可能被多次调用，如果当前没有数据，clear与否都一样
        if len(res) == 0: return res

        if clear_cache: self.clear_cache()
        return res

    def _get_before_last_level_data(self, k):
        ''' a.b -> (self._data[a], b) '''
        data = self._data
        k_list = k.split('.')
        for name in k_list[0:-1]: data = data[name]
        return (data, k_list[-1])

    def _get_level_data(self, k):
        ''' a.b -> self._data[a][b] '''
        data = self._data
        k_list = k.split('.')
        for name in k_list: data = data[name]
        return data

    def remove_data(self, k):
        try:
            if self._remove_data_impl(k):
                self.remove_cache_data(k)
                return True
            else:
                return False
        except Exception, e:
            return False

    def _remove_data_impl(self, k):
        bld, k_last = self._get_before_last_level_data(k)
        if k_last not in bld: return False

        bld.pop(k_last)
        return True

    def remove_cache_data(self, k):
        if not self._can_cache: return

        k_list = k.split('.')

        if _delete_dict_recursive(self._update, k_list):
            _set_dict_recursive(self._remove, k_list)

    def update_data(self, k, v):
        try:
            if self._update_data_impl(k, v):
                self.update_cache_data(k)
                return True
            else:
                return False
        except Exception, e:
            return False

    def _update_data_impl(self, k, v):
        if isinstance(v, ChangeDataCacheDictItem) or isinstance(v, ChangeDataCacheListItem):
            v = v._data

        bld, k_last = self._get_before_last_level_data(k)
        bld[k_last] = v
        return True

    def update_cache_data(self, k):
        if not self._can_cache: return

        k_list = k.split('.')

        # 如果之前删除，后面又更新，则覆盖删除，使用更新数据重新来过
        if _delete_dict_recursive(self._remove, k_list):
            # update中记录的是嵌套结构的dict数据结构
            _set_dict_recursive(self._update, k_list)

    def pull_data(self, k, v):
        try:
            if self._pull_data_impl(k, v):

                # list 只要是修改，统一使用 update 逻辑来记录
                self.update_cache_data(k)
                return True
            else:
                return False

        except Exception, e:
            return False

    def _pull_data_impl(self, k, v):
        ld = self._get_level_data(k)
        if not isinstance(ld, list): return False
        if v not in ld: return False

        ld.remove(v)
        return True

    def push_data(self, k, v, push_to_set=False):
        try:
            if self._push_data_impl(k, v, push_to_set=push_to_set):
                self.update_cache_data(k)
                return True
            else:
                return False
        except Exception, e:
            return False

    def _push_data_impl(self, k, v, push_to_set=False):
        ld = self._get_level_data(k)
        if not isinstance(ld, list): return False

        # @note 只有是push_to_set且数据已经存在的情况下，push失败
        if push_to_set and (v in ld): return False

        ld.append(v)
        return True

    def __str__(self):
        return 'data : %s\nupdate : %s\nremove : %s' % (self._data, self._update, self._remove)

    # @note 这里的k，必须是第一层级的key，即如果k为 'a.b' ，那么认为 'a.b' 就是实际的key，而不是按照点号分割的方式得到嵌套层级数据
    def get_data(self, k):
        try:
            v = self._data[k]
            if isinstance(v, list):
                return ChangeDataCacheListItem(self, k, v)
            elif isinstance(v, dict):
                return ChangeDataCacheDictItem(self, k, v)
            else:
                return v
        except Exception, e:
            return None

    # @note 这里的k，必须是第一层级的key，即如果k为 'a.b' ，那么认为 'a.b' 就是实际的key，而不是按照点号分割的方式得到嵌套层级数据
    def set_default_data(self, k, default_value):
        if k not in self._data:
            self.update_data(k, default_value)

        return self.get_data(k)
