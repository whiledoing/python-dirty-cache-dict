# python-dirty-cache-dict

python dict-like data structure which can automatic trace change/remove information of the dict.

# example

```python
# ChangeDataCache is a wrapper class of original python dict
data = ChangeDataCache({'base': {'money': 100}})

# type of base_dict is ChangeDataCacheDictItem, which can trace the change/remove information on dict-like operation
base_dict = data.get_data('base')
base_dict['money'] = 100
base_dict.setdefault('props', {})['weapon_1'] = 10
base_dict.setdefault('props', {})['weapon_2'] = 20
base_dict['props'].pop('weapon_1')

# after a lot of operation, you can get the change cache in a compact version
change_cache = data.pack_cache()

# the cache can be used in synchronization or maybe dump into db
mongo_client.update(change_cache)
```
