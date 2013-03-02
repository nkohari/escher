local leveldb = require("leveldb")

describe("dat leveldb", function()
  local db

  setup(function()
    db = leveldb("test.db", {create_if_missing=true})
    db:put("a", "one")
    db:put("b", "two")
    db:put("c", "three")
  end)

  describe("a call to get()", function()
    describe("for an existing key", function()
      it("should return the value", function()
        assert.are_equal(db:get("a"), "one")
      end)
    end)
    describe("for an non-existent key", function()
      it("should return nil", function()
        assert.is_falsy(db:get("wat"))
      end)
    end)
  end)

end)
