
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
	c:shrink(max_size, max_count)
	c.max_size
	c.max_count
	c.size
	c.count

	c:get(k) -> &v|nil
	c:put(k, v) -> ok?

]]

if not ... then require'lrucache_test'; return end

local linkedlist = require'linkedlist'

local function memsize_func(T)
	return T:isstruct() and T.methods.__memsize
		or macro(function() return sizeof(T) end)
end

local function cache_type(key_t, val_t, hash, equal, size_t, C)

	setfenv(1, C)

	val_t = val_t or bool --TODO: make val_t truly optional.

	local struct pair_t {
		key: key_t;
		val: val_t;
	}

	local pair_list = linkedlist {T = pair_t, C = C}

	--TODO: wrap the cache struct in an opaque struct so that we can make
	--indices.userdata = &self.lru and use that instead of this brittle hack.
	local deref = macro(function(self, pi)
		return `&(([&pair_list](([&int8](self))-sizeof(pair_list))):at(@pi).key)
	end)

	local indices_set = map {
		key_t = size_t, deref = deref, deref_key_t = key_t,
		hash = hash, equal = equal, C = C
	}

	local struct cache {
		max_size: size_t;
		max_count: size_t;
		size: size_t;
		count: size_t;
		lru: pair_list;        --{{key=k, val=v}, ...}
		indices: indices_set;  --{k/i}
	}

	--key/value pair interface
	local key_memsize = memsize_func(key_t)
	local val_memsize = memsize_func(val_t)
	local pair_memsize = macro(function(k, v)
		local fixed_size = sizeof(&key_t) + 2 * sizeof(size_t)
		return `key_memsize(k) + val_memsize(v) + fixed_size
	end)

	local free_key = key_t:isstruct() and key_t.methods.free or noop
	local free_val = val_t:isstruct() and val_t.methods.free or noop

	local free_pairs = macro(function(self, k, v)
		if free_key == noop and free_val == noop then
			return quote end
		end
		return quote
			for e in self.lru do
				free_key(&e.key)
				free_val(&e.val)
			end
		end
	end)

	--storage

	function cache.metamethods.__cast(from, to, exp)
		if from == niltype or from:isunit() then
			return `cache {max_size=0, max_count=[size_t:max()], size=0,
					lru=nil, indices=nil}
		else
			error'invalid cast'
		end
	end

	terra cache:free() --can be reused after free
		free_pairs(self)
		self.lru:free()
		self.indices:free()
		self.size = 0
		self.count = 0
	end

	terra cache:clear()
		free_pairs(self)
		self.lru:clear()
		self.indices:clear()
		self.size = 0
		self.count = 0
	end

	terra cache:preallocate(count: size_t)
		self.lru:preallocate(count)
		self.indices:preallocate(count)
	end

	terra cache:__memsize()
		return sizeof(cache)
			+ self.lru:__memsize()
			+ self.indices:__memsize()
	end

	--operation

	terra cache:get(k: key_t): &pair_t
		var ki = self.indices:get_index(k)
		if ki == -1 then return nil end
		var i = self.indices:noderef_key_at_index(ki)
		var pair = self.lru:at(i)
		self.lru:make_first(i)
		return pair
	end

	terra cache:_remove_at(i: size_t)
		var pair = self.lru:at(i)
		var pair_size: size_t = pair_memsize(&pair.key, &pair.val)
		free_key(&pair.key)
		free_val(&pair.val)
		self.lru:remove(i)
		assert(self.indices:del(pair.key))
		self.size = self.size - pair_size
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
		var pair_size: size_t = pair_memsize(&k, &v)
		self:shrink(self.max_size - pair_size, self.max_count - 1)
		var i = self.lru:insert_first()
		if i == -1 then return nil end
		var p = self.lru:at(i)
		assert(p ~= nil)
		p.key, p.val = k, v
		var ret = self.indices:putifnew(i, true) --fails if the key is present!
		if ret == -1 then self.lru:remove(i); return nil end
		self.size = self.size + pair_size
		self.count = self.count + 1
		return p
	end

	terra cache:remove(k: key_t): bool
		var ki = self.indices:get_index(k)
		if ki == -1 then return false end
		var i = self.indices:noderef_key_at_index(ki)
		self.lru:remove(i)
		self.indices:del_at_index(ki)
		return true
	end

	return cache
end
cache_type = terralib.memoize(cache_type)

local cache_type = function(key_t, val_t, hash, equal, size_t, C)
	if terralib.type(key_t) == 'table' then
		local t = key_t
		key_t, val_t, hash, size_t, C =
			t.key_t, t.val_t, t.hash, t.size_t, t.C
	end
	size_t = size_t or int
	C = C or require'low'
	return cache_type(key_t, val_t, hash, equal, size_t, C)
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
