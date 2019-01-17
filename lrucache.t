
--LRU cache for Terra, size-limited and count-limited.
--Written by Cosmin Apreutesei. Public Domain.

--[[  API

	local C = cache{key_t=, val_t=, size_t=int, C=require'low'}
	var c = cache(key_t, val_t, size_t=int) -- preferred variant
	var c: C = nil   -- =nil is important!
	var c = C(nil)   -- (nil) is important!
	c:free()
	c:clear()
	c:preallocate(size) -> ok?
	c.max_size
	c.max_count
	c.size
	c.count

	c:get(k) -> &v|nil
	c:put(k, v) -> ok?

]]

if not ... then require'lrucache_test'; return end

local linkedlist = require'linkedlist'

local function cache_type(key_t, val_t, size_t, C)

	setfenv(1, C)

	local values_list = linkedlist{T = val_t, C = C}
	local indices_map = map{key_t = key_t, val_t = size_t, C = C}
	local keys_map = map{key_t = size_t, val_t = key_t, C = C}

	local struct cache {
		max_size: size_t;
		max_count: size_t;
		size: size_t;
		count: size_t;
		lru: values_list;      --{i -> v}
		indices: indices_map;  --{k -> i}
		keys: keys_map;        --{i -> k}
	}

	--memory management

	function cache.metamethods.__cast(from, to, exp)
		if from == niltype or from:isunit() then
			return `cache {max_size=0, max_count=[size_t:max()], size=0,
				lru=nil, indices=nil, keys=nil}
		else
			error'invalid cast'
		end
	end

	terra cache:free() --can be reused after free
		self.lru:free()
		self.indices:free()
		self.keys:clear()
		self.size = 0
		self.count = 0
	end

	terra cache:clear()
		self.lru:clear()
		self.indices:clear()
		self.keys:clear()
		self.size = 0
		self.count = 0
	end

	terra cache:preallocate(count: size_t)
		self.lru:preallocate(count)
		self.indices:preallocate(count)
		self.keys:preallocate(count)
	end

	--operation

	terra cache:get(key: key_t): &val_t
		var i = self.indices(key, -1)
		if i == -1 then return nil end
		var v = self.lru:at(i)
		self.lru:make_first(i)
		return v
	end

	terra cache:_remove_at(i: size_t)
		var v = self.lru:at(i)
		var v_size = v:lrucache_size()
		self.lru:remove(i)
		var ki = self.keys:get_index(i)
		assert(self.indices:del(self.keys:val_at(ki)))
		assert(self.keys:del_at(ki))
		self.size = self.size - v_size
		self.count = self.count - 1
	end

	terra cache:shrink(max_size: size_t, max_count: size_t)
		while self.size > max_size or self.count > max_count do
			var i = self.lru.last_index
			if i == -1 then return false end
			self:_remove_at(i)
		end
		return true
	end

	terra cache:put(k: key_t, v: val_t)
		var v_size: size_t = v:lrucache_size()
		self:shrink(self.max_size - v_size, self.max_count - 1)
		var i = self.lru:insert_first(v)
		if i == -1 then return false end
		var ii = self.indices:putifnew(k, i) --fails if the key is present!
		if ii == -1 then self.lru:remove(i); return false end
		var ki = self.keys:put(i, k)
		if ki == -1 then self.lru:remove(i); self.indices:del_at(ii); return false end
		self.size = self.size + v_size
		self.count = self.count + 1
		return true
	end

	return cache
end
cache_type = terralib.memoize(cache_type)

local cache_type = function(key_t, val_t, size_t, C)
	if terralib.type(key_t) == 'table' then
		local t = key_t
		key_t, val_t, size_t, C = t.key_t, t.val_t, t.size_t, t.C
	end
	size_t = size_t or int
	C = C or require'low'
	return cache_type(key_t, val_t, size_t, C)
end

local cache_type = macro(
	--calling it from Terra returns a new cache object.
	function(max_size, max_count, key_t, val_t, size_t)
		local cache_t = cache_type(key_t, val_t, size_t)
		size_t = size_t or int
		max_size = max_size or size_t:astype():max()
		max_count = max_count or size_t:astype():max()
		return quote
			var cache: cache_t = nil
			cache.max_size = max_size
			cache.max_count = max_count
			in cache
		end
	end,
	--calling it from Lua or from an escape or in a type declaration returns
	--just the type, and you can also pass a custom C namespace.
	cache_type
)

return cache_type
