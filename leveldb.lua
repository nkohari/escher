local ffi = require("ffi")
local leveldb = ffi.load("leveldb")

ffi.cdef [[
  typedef struct leveldb_t leveldb_t;
  typedef struct leveldb_options_t leveldb_options_t;
  typedef struct leveldb_readoptions_t leveldb_readoptions_t;
  typedef struct leveldb_writeoptions_t leveldb_writeoptions_t;
  typedef struct leveldb_writebatch_t leveldb_writebatch_t;
  typedef struct leveldb_iterator_t leveldb_iterator_t;

  leveldb_t* leveldb_open(const leveldb_options_t* options, const char* name, char** errptr);
  void leveldb_close(leveldb_t* db);
  void leveldb_destroy_db(const leveldb_options_t* options, const char* name, char** errptr);
  void leveldb_repair_db(const leveldb_options_t* options, const char* name, char** errptr);

  char* leveldb_get(leveldb_t* db, const leveldb_readoptions_t* options, const char* key, size_t keylen, size_t* vallen, char** errptr);
  void leveldb_put(leveldb_t* db, const leveldb_writeoptions_t* options, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);
  void leveldb_delete(leveldb_t* db, const leveldb_writeoptions_t* options, const char* key, size_t keylen, char** errptr);

  leveldb_writebatch_t* leveldb_writebatch_create();
  void leveldb_writebatch_put(leveldb_writebatch_t*, const char* key, size_t klen, const char* val, size_t vlen);
  void leveldb_writebatch_delete(leveldb_writebatch_t*, const char* key, size_t klen);
  void leveldb_write(leveldb_t* db, const leveldb_writeoptions_t* options, leveldb_writebatch_t* batch, char** errptr);

  leveldb_iterator_t* leveldb_create_iterator(leveldb_t* db, const leveldb_readoptions_t* options);
  void leveldb_iter_destroy(leveldb_iterator_t*);
  unsigned char leveldb_iter_valid(const leveldb_iterator_t*);
  void leveldb_iter_seek_to_first(leveldb_iterator_t*);
  void leveldb_iter_seek_to_last(leveldb_iterator_t*);
  void leveldb_iter_seek(leveldb_iterator_t*, const char* k, size_t klen);
  void leveldb_iter_next(leveldb_iterator_t*);
  void leveldb_iter_prev(leveldb_iterator_t*);
  const char* leveldb_iter_key(const leveldb_iterator_t*, size_t* klen);
  const char* leveldb_iter_value(const leveldb_iterator_t*, size_t* vlen);
  void leveldb_iter_get_error(const leveldb_iterator_t*, char** errptr);

  leveldb_options_t* leveldb_options_create();
  void leveldb_options_set_create_if_missing(const leveldb_options_t* options, unsigned char);
  void leveldb_options_set_error_if_exists(const leveldb_options_t* options, unsigned char);
  void leveldb_options_set_compression(const leveldb_options_t* options, int);
  void leveldb_options_destroy(leveldb_options_t*);

  leveldb_readoptions_t* leveldb_readoptions_create();
  void leveldb_readoptions_destroy(leveldb_readoptions_t*);

  leveldb_writeoptions_t* leveldb_writeoptions_create();
  void leveldb_writeoptions_destroy(leveldb_writeoptions_t*);

  void leveldb_free(void* ptr);
  int leveldb_major_version();
  int leveldb_minor_version();
]]

local function create_options(options)
  options = options or {}
  local c_options = leveldb.leveldb_options_create()
  if options.create_if_missing then leveldb.leveldb_options_set_create_if_missing(c_options, 1) end
  if options.error_if_exists   then leveldb.leveldb_options_set_error_if_exists(c_options, 1)   end
  if options.compression       then leveldb.leveldb_options_set_compression(c_options, 1)       end
  return c_options
end

local function create_read_options(options)
  options = options or {}
  local c_options = leveldb.leveldb_readoptions_create()
  return c_options
end

local function create_write_options(options)
  options = options or {}
  local c_options = leveldb.leveldb_writeoptions_create()
  return c_options
end

DB = {}

function DB:new(filename, options)
  obj = {}
  setmetatable(obj, self)

  self.__index = self
  self.__gc = function()
    if self._db then
      leveldb.leveldb_free(self._db)
    end
  end

  local c_options = create_options(options)
  local c_err = ffi.new("char*[1]")
  self._db = leveldb.leveldb_open(c_options, filename, c_err)
  leveldb.leveldb_options_destroy(c_options)
  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  end

  local major = leveldb.leveldb_major_version()
  local minor = leveldb.leveldb_minor_version()
  self.version = major .. "." .. minor

  return obj
end

function DB:close()
  leveldb.leveldb_close(self._db)
end

function DB:get(key, options)
  local c_options = create_read_options(options)
  local c_err = ffi.new("char*[1]")
  local c_size = ffi.new("size_t[1]")
  local c_result = leveldb.leveldb_get(self._db, c_options, key, #key, c_size, c_err)
  leveldb.leveldb_readoptions_destroy(c_options)
  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  elseif c_size[0] == 0 then
    return nil
  else
    return ffi.string(c_result, c_size[0])
  end
end

function DB:put(key, val, options)
  local c_options = create_write_options(options)
  local c_err = ffi.new("char*[1]")
  leveldb.leveldb_put(self._db, c_options, key, #key, val, #val, c_err)
  leveldb.leveldb_writeoptions_destroy(c_options)
  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  end
end

function DB:delete(key, options)
  local c_options = create_write_options(options)
  local c_err = ffi.new("char*[1]")
  leveldb.leveldb_delete(self._db, c_options, key, #key, c_err)
  leveldb.leveldb_writeoptions_destroy(c_options)
  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  end
end

function DB:batch(options)
  return WriteBatch:new(self._db, options)
end

function DB:iterator(options)
  return Iterator:new(self._db, options)
end

WriteBatch = {}

function WriteBatch:new(db, options)
  obj = {}
  setmetatable(obj, self)

  self.__index = self
  self.__gc = function()
    if self._batch then
      leveldb.leveldb_free(self._batch)
    end
    if self._options then
      leveldb.leveldb_writeoptions_destroy(self._options)
    end
  end

  self._db = db
  self._options = create_write_options(options)
  self._batch = leveldb.leveldb_writebatch_create()

  return obj
end

function WriteBatch:put(key, val)
  leveldb.leveldb_writebatch_put(self._batch, key, #key, val, #val)
end

function WriteBatch:delete(key)
  leveldb.leveldb_writebatch_delete(self._batch, key, #key)
end

function WriteBatch:write()
  local c_err = ffi.new("char*[1]")
  leveldb.leveldb_write(self._db, self._options, self._batch, c_err)
  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  end
end

Iterator = {}

function Iterator:new(db, options)
  obj = {}
  setmetatable(obj, self)

  self.__index = self
  self.__gc = function()
    if self._iterator then
      leveldb.leveldb_iter_destroy(self._iterator)
    end
    if self._options then
      leveldb.leveldb_readoptions_destroy(self._options)
    end
  end

  self._db = db
  self._options = create_read_options(options)
  self._iterator = leveldb.leveldb_create_iterator(self._db, self._options)

  return obj
end

function Iterator:first()
  leveldb.leveldb_iter_seek_to_first(self._iterator)
end

function Iterator:last()
  leveldb.leveldb_iter_seek_to_last(self._iterator)
end

function Iterator:seek(key)
  leveldb.leveldb_iter_seek(self._iterator, key, #key)
end

function Iterator:next()
  leveldb.leveldb_iter_next(self._iterator)
  
  local valid = leveldb.leveldb_iter_valid(self._iterator)
  if valid == 0 then
    return nil
  end

  local c_key_size = ffi.new("size_t[1]")
  local c_key = leveldb.leveldb_iter_key(self._iterator, c_key_size)
  local key = ffi.string(c_key, c_key_size[0])

  local c_value_size = ffi.new("size_t[1]")
  local c_value = leveldb.leveldb_iter_value(self._iterator, c_value_size)
  local value = ffi.string(c_value, c_value_size[0])

  return key, value
end

Module = {}

Module.open = function(filename, options)
  return DB:new(filename, options)
end

Module.destroy = function(filename, options)
  local c_options = create_options(options)
  local c_err = ffi.new("char*[1]")

  leveldb.leveldb_destroy_db(c_options, filename, c_err)
  leveldb.leveldb_options_destroy(c_options)

  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  end
end

Module.repair = function(filename, options)
  local c_options = create_options(options)
  local c_err = ffi.new("char*[1]")

  leveldb.leveldb_repair_db(c_options, filename, c_err)
  leveldb.leveldb_options_destroy(c_options)

  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  end
end

return Module
