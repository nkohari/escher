local ffi = require("ffi")
local leveldb = ffi.load("leveldb")

ffi.cdef [[
  typedef struct leveldb_t leveldb_t;
  typedef struct leveldb_options_t leveldb_options_t;
  typedef struct leveldb_readoptions_t leveldb_readoptions_t;
  typedef struct leveldb_writeoptions_t leveldb_writeoptions_t;
  typedef struct leveldb_writebatch_t leveldb_writebatch_t;

  leveldb_t* leveldb_open(const leveldb_options_t* options, const char* name, char** errptr);
  void leveldb_close(leveldb_t* db);
  void leveldb_free(void* ptr);

  char* leveldb_get(leveldb_t* db, const leveldb_readoptions_t* options, const char* key, size_t keylen, size_t* vallen, char** errptr);
  void leveldb_put(leveldb_t* db, const leveldb_writeoptions_t* options, const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);
  void leveldb_delete(leveldb_t* db, const leveldb_writeoptions_t* options, const char* key, size_t keylen, char** errptr);

  leveldb_writebatch_t* leveldb_writebatch_create();
  void leveldb_writebatch_put(leveldb_writebatch_t*, const char* key, size_t klen, const char* val, size_t vlen);
  void leveldb_writebatch_delete(leveldb_writebatch_t*, const char* key, size_t klen);
  void leveldb_write(leveldb_t* db, const leveldb_writeoptions_t* options, leveldb_writebatch_t* batch, char** errptr);

  leveldb_iterator_t* leveldb_create_iterator(leveldb_t* db, const leveldb_readoptions_t* options);

  leveldb_options_t* leveldb_options_create();
  void leveldb_options_set_create_if_missing(const leveldb_options_t* options, unsigned char);
  void leveldb_options_set_error_if_exists(const leveldb_options_t* options, unsigned char);
  void leveldb_options_destroy(leveldb_options_t*);

  leveldb_readoptions_t* leveldb_readoptions_create();
  void leveldb_readoptions_destroy(leveldb_readoptions_t*);

  leveldb_writeoptions_t* leveldb_writeoptions_create();
  void leveldb_writeoptions_destroy(leveldb_writeoptions_t*);
]]

local function create_open_options(options)
  options = options or {}
  local c_options = leveldb.leveldb_options_create()
  if options.create_if_missing then leveldb.leveldb_options_set_create_if_missing(c_options, 1) end
  if options.error_if_exists   then leveldb.leveldb_options_set_error_if_exists(c_options, 1)   end
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

  local c_options = create_open_options(options)
  local c_err = ffi.new("char*[1]")
  self._db = leveldb.leveldb_open(c_options, filename, c_err)
  leveldb.leveldb_options_destroy(c_options)
  if c_err[0] ~= nil then
    error(ffi.string(c_err[0]))
  end

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
  local c_options = create_write_options(options)
  return WriteBatch:new(self._db, c_options)
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
  self._options = options
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

--extern leveldb_iterator_t* leveldb_create_iterator(leveldb_t* db, const leveldb_readoptions_t* options);

function Iterator:new(db, options)
  obj = {}
  setmetatable(obj, self)

  self.__index = self
  self.__gc = function()
    if self._iterator then
      leveldb.leveldb_free(self._iterator)
    end
  end

  self._db = db
  local c_options = create_read_options(options)
  self._iterator = leveldb.leveldb_create_iterator(db, c_options)
  leveldb.leveldb_readoptions_destroy(c_options)

  return obj
end

return function(filename, options)
  return DB:new(filename, options)
end
