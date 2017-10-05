package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertBuilderToSql(t *testing.T) {
	b := Insert("").
		Prefix("WITH prefix AS ?", 0).
		Into("a").
		Options("DELAYED", "IGNORE").
		Columns("b", "c").
		Values(1, 2).
		Values(3, Expr("? + 1", 4)).
		Suffix("RETURNING ?", 5)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"INSERT DELAYED IGNORE INTO a (b,c) VALUES (?,?),(?,? + 1) " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 2, 3, 4, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderToSqlErr(t *testing.T) {
	_, _, err := Insert("").Values(1).ToSql()
	assert.Error(t, err)

	_, _, err = Insert("x").ToSql()
	assert.Error(t, err)
}

func TestInsertBuilderPlaceholders(t *testing.T) {
	b := Insert("test").Values(1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES (?,?)", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES ($1,$2)", sql)
}

func TestInsertBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Insert("test").Values(1).RunWith(db)

	expectedSql := "INSERT INTO test VALUES (?)"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestInsertBuilderNoRunner(t *testing.T) {
	b := Insert("test").Values(1)

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestInsertBuilderSetMap(t *testing.T) {
	b := Insert("table").SetMap(Eq{"field1": 1})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "INSERT INTO table (field1) VALUES (?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderSelect(t *testing.T) {
	sb := Select("field1").From("table1").Where(Eq{"field1": 1})
	ib := Insert("table2").Columns("field1").Select(sb)

	sql, args, err := ib.ToSql()
	assert.NoError(t, err)

	expectedSql := "INSERT INTO table2 (field1) SELECT field1 FROM table1 WHERE field1 = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderUseDefault(t *testing.T) {
	b := Insert("table").SetMap(Eq{"`field1`": 1, "`field2`": KeywordDefault})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expect := []string{"INSERT INTO table (`field1`,`field2`) VALUES (?,DEFAULT)", "INSERT INTO table (`field2`,`field1`) VALUES (DEFAULT,?)"}
	if sql != expect[0] && sql != expect[1] {
		t.Errorf("expected one of %#v, got %#v", expect, sql)
	}

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilder_SetMap(t *testing.T) {
	b := Insert("table").SetMap(Eq{"`field1`": 11, "`field2`": 12})

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expect := []string{"INSERT INTO table (`field1`,`field2`) VALUES (?,?)", "INSERT INTO table (`field2`,`field1`) VALUES (?,?)"}
	if sql != expect[0] && sql != expect[1] {
		t.Errorf("expected one of %#v, got %#v", expect, sql)
	}
}

func TestInsertBuilder_AddMap(t *testing.T) {
	b := Insert("table").AddMap(Eq{"`field1`": 11, "`field2`": 12})
	b.AddMap(Eq{"`field2`": 22, "`field1`": 21})

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expect := []string{"INSERT INTO table (`field1`,`field2`) VALUES (?,?),(?,?)", "INSERT INTO table (`field2`,`field1`) VALUES (?,?),(?,?)"}
	if sql != expect[0] && sql != expect[1] {
		t.Errorf("expected one of %#v, got %#v", expect, sql)
	}
}

func TestInsertBuilder_SetMapAddMap(t *testing.T) {
	b := Insert("table").SetMap(Eq{"`field1`": 11, "`field2`": 12})
	b.AddMap(Eq{"`field2`": 22, "`field1`": 21})

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expect := []string{"INSERT INTO table (`field1`,`field2`) VALUES (?,?),(?,?)", "INSERT INTO table (`field2`,`field1`) VALUES (?,?),(?,?)"}
	if sql != expect[0] && sql != expect[1] {
		t.Errorf("expected one of %#v, got %#v", expect, sql)
	}
}
