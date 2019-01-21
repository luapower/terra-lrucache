
setfenv(1, require'low')
local cache = require'lrucache'

local S = struct { x: int }

S.methods.__memsize = terra(self: &S)
	return 100
end

local cache = cache{key_t = int, val_t = S}
terra test()
	var cache: cache = nil
	cache.max_size = 250
	print(cache:put(5, S{15}))
	print('has', cache.indices:has(5))
	print(cache:put(7, S{22}))
	print(cache:put(3, S{55})) --remove 5
	print()
	print(cache.count, cache.size)
	print(cache:get(5), cache:get(7), cache:get(3))
end
test()

--f = terra() end
--terra f() end
--terra f() end
