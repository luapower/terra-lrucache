--[[

	LRU cache for Terra, size-limited and count-limited.
	Written by Cosmin Apreutesei. Public Domain.

	Breaks if trying to put a key/val pair whose memsize is > max_size.

	local C = cache{key_t=,val_t=,size_t=int} create type from Lua
	var c = cache(key_t,val_t,[size_t=int])   create value from Terra
	c:init()                                  initialize (for struct members)
	c:free()                                  free
	c:clear()                                 clear (but preserve memory)
	c.min_capacity = n                        (write/only) preallocate a number of items
	c:shrink(max_size, max_count)             shrink (but don't free memory)
	c.max_size                                (read/write) max bytesize
	c.max_count                               (read/write) max number of items
	c.size                                    (read/only) current size
	c.count                                   (read/only) current number of items

	c:get(k) -> &v|nil                        get value
	c:put(k, v) -> pair|nil                   put value (nil if value > max_size)

]]

if not ... then require'lrucache_test'; return end

setfenv(1, require'low')
local linkedlist = require'linkedlist'

local function cache_type(key_t, val_t, size_t, hash, equal)

	val_t = val_t or tuple()

	local struct pair {
		key: key_t;
		val: val_t;
	}

	local pair_list = linkedlist{T = pair}

	local deref = macro(function(self, i)
		return `&self.state:link(@i).item.key
	end)

	local indices_set = map{
		key_t = size_t,
		hash = hash, equal = equal,
		deref = deref, deref_key_t = key_t,
		state_t = &pair_list;
	}

	local struct cache (gettersandsetters) {
		max_size: size_t;
		max_count: size_t;
		size: size_t;
		count: size_t;
		lru: pair_list;       --linked list of key/val pairs
		indices: indices_set; --set of indices hashed by key_t through deref().
	}

	cache.key_t = key_t
	cache.val_t = val_t
	cache.size_t = size_t

	local pair_memsize = macro(function(k, v)
		return `memsize(k) + memsize(v)
			+ sizeof(pair_list.link) - sizeof(key_t) - sizeof(val_t)
	end)

	--storage

	terra cache:init()
		fill(self)
		self.max_count = [size_t:max()]
		self.lru:init()
		self.indices:init()
		self.indices.state = &self.lru
	end

	cache.methods._free_pairs = macro(function(self)
		if getmethod(key_t, 'free') or getmethod(val_t, 'free') then
			return quote
				for i,link in self.lru do
					call(link.item.key, 'free')
					call(link.item.val, 'free')
				end
			end
		else
			return quote end
		end
	end)

	terra cache:clear()
		self:_free_pairs()
		self.lru:clear()
		self.indices:clear()
		self.size = 0
		self.count = 0
	end

	terra cache:free()
		self:clear()
		self.lru:free()
		self.indices:free()
	end

	terra cache:setcapacity(n: size_t)
		return self.lru:setcapacity(n)
			and self.indices:resize(n)
	end
	terra cache:set_capacity(n: size_t)
		self.lru.capacity = n
		self.indices.capacity = n
	end
	terra cache:set_min_capacity(n: size_t)
		self.lru.min_capacity = n
		self.indices.min_capacity = n
	end

	terra cache:__memsize()
		return sizeof(cache)
			- sizeof(self.lru) + memsize(self.lru)
			- sizeof(self.indices) + memsize(self.indices)
	end

	--operation

	terra cache:get(k: key_t)
		var ki = self.indices:index(k, -1)
		if ki == -1 then return nil end
		var i = self.indices:noderef_key_at_index(ki)
		--self.lru:move_before(self.lru.first, i)
		return &self.lru:link(i).item
	end

	terra cache:shrink(max_size: size_t, max_count: size_t)
		while self.size > max_size or self.count > max_count do
			var i = self.lru.last
			assert(i ~= -1) --if it's empty we shouldn't be here
			var pair = self.lru:link(i).item
			var pair_size = pair_memsize(pair.key, pair.val)
			assert(self.indices:del(pair.key))
			call(pair.key, 'free')
			call(pair.val, 'free')
			self.lru:remove(i)
			self.size = self.size - pair_size
			self.count = self.count - 1
		end
	end

	terra cache:put(k: key_t, v: val_t)
		var pair_size = pair_memsize(k, v)
		self:shrink(self.max_size - pair_size, self.max_count - 1)
		var i, link = self.lru:insert_before(self.lru.first)
		link.item.key = k
		link.item.val = v
		assert(self.indices:add(i) ~= -1) --fails if the key is present!
		self.size = self.size + pair_size
		self.count = self.count + 1
		return &link.item
	end

	terra cache:remove(k: key_t): bool
		var ki = self.indices:index(k, -1)
		if ki == -1 then return false end
		var i = self.indices:noderef_key_at_index(ki)
		self.lru:remove(i)
		self.indices:del_at_index(ki)
		return true
	end

	return cache
end
cache_type = terralib.memoize(cache_type)

local cache_type = function(key_t, val_t, size_t)
	local hash, equal
	if terralib.type(key_t) == 'table' then
		local t = key_t
		key_t, val_t, size_t = t.key_t, t.val_t, t.size_t
		hash, equal = t.hash, t.equal
	end
	size_t = size_t or int
	return cache_type(key_t, val_t, size_t, hash, equal)
end

local cache_type = macro(
	--calling it from Terra returns a new cache object.
	function(key_t, val_t, size_t)
		local cache_type = cache_type(key_t, val_t, size_t)
		return quote var c: cache; c:init() in c end
	end,
	--calling it from Lua or from an escape or in a type declaration returns
	--just the type, and you can also pass a custom C namespace.
	cache_type
)

return cache_type
