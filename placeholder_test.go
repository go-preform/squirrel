package squirrel

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuestion(t *testing.T) {
	sql := "x = ? AND y = ?"
	s, _ := Question.ReplacePlaceholders(sql)
	assert.Equal(t, sql, s)
}

func TestDollar(t *testing.T) {
	sql := "x = ? AND y = ?"
	s, _ := Dollar.ReplacePlaceholders(sql)
	assert.Equal(t, "x = $1 AND y = $2", s)
}

func TestColon(t *testing.T) {
	sql := "x = ? AND y = ?"
	s, _ := Colon.ReplacePlaceholders(sql)
	assert.Equal(t, "x = :1 AND y = :2", s)
}

func TestAtp(t *testing.T) {
	sql := "x = ? AND y = ?"
	s, _ := AtP.ReplacePlaceholders(sql)
	assert.Equal(t, "x = @p1 AND y = @p2", s)
}

func TestPlaceholders(t *testing.T) {
	assert.Equal(t, Placeholders(2), "?,?")
}

func TestEscapeDollar(t *testing.T) {
	sql := "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ??| array['?'] AND enabled = ?"
	s, _ := Dollar.ReplacePlaceholders(sql)
	assert.Equal(t, "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ?| array['$1'] AND enabled = $2", s)
}

func TestEscapeColon(t *testing.T) {
	sql := "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ??| array['?'] AND enabled = ?"
	s, _ := Colon.ReplacePlaceholders(sql)
	assert.Equal(t, "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ?| array[':1'] AND enabled = :2", s)
}

func TestEscapeAtp(t *testing.T) {
	sql := "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ??| array['?'] AND enabled = ?"
	s, _ := AtP.ReplacePlaceholders(sql)
	assert.Equal(t, "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ?| array['@p1'] AND enabled = @p2", s)
}

func TestReplacePositionalPlaceholdersFast(t *testing.T) {
	var (
		query string
		err   error
	)
	query, err = replacePositionalPlaceholdersFast("SELECT \"TestBS\".\"id\" AS \"Id\", \"TestBS\".\"a_id\" AS \"AId\", \"TestBS\".\"name\" AS\"Name\", \"TestBS\".\"int4\" AS \"Int4\", \"TestBS\".\"int8\" AS \"Int8\", \"TestBS\".\"float4\"AS \"Float4\", \"TestBS\".\"float8\" AS \"Float8\", \"TestBS\".\"bool\" AS \"Bool\", \"TestBS\".\"text\" AS \"Text\", \"TestBS\".\"time\" AS \"Time\" FROM \"preform_benchmark\".\"test_b\" AS \"TestBS\" WHERE (\"TestBS\".\"a_id\" IN ('??',?,?,?))??", "$")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT \"TestBS\".\"id\" AS \"Id\", \"TestBS\".\"a_id\" AS \"AId\", \"TestBS\".\"name\" AS\"Name\", \"TestBS\".\"int4\" AS \"Int4\", \"TestBS\".\"int8\" AS \"Int8\", \"TestBS\".\"float4\"AS \"Float4\", \"TestBS\".\"float8\" AS \"Float8\", \"TestBS\".\"bool\" AS \"Bool\", \"TestBS\".\"text\" AS \"Text\", \"TestBS\".\"time\" AS \"Time\" FROM \"preform_benchmark\".\"test_b\" AS \"TestBS\" WHERE (\"TestBS\".\"a_id\" IN ('?',$1,$2,$3))?", query)
	query, err = replacePositionalPlaceholdersFast("SELECT * FROM \"preform_benchmark\".\"test_b\" AS \"TestBS\" WHERE \"TestBS\".\"a_id\" > ?", "$")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM \"preform_benchmark\".\"test_b\" AS \"TestBS\" WHERE \"TestBS\".\"a_id\" > $1", query)
}
func BenchmarkPlaceholdersArray(b *testing.B) {
	var count = b.N
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}
	var _ = strings.Join(placeholders, ",")
}

func BenchmarkPlaceholdersStrings(b *testing.B) {
	Placeholders(b.N)
}

func BenchmarkReplacePositionalPlaceholders(b *testing.B) {
	var (
		query string
		err   error
	)
	for i := 0; i < b.N; i++ {
		query, err = replacePositionalPlaceholders("SELECT \"TestBS\".\"id\" AS \"Id\", \"TestBS\".\"a_id\" AS \"AId\", \"TestBS\".\"name\" AS\"Name\", \"TestBS\".\"int4\" AS \"Int4\", \"TestBS\".\"int8\" AS \"Int8\", \"TestBS\".\"float4\"AS \"Float4\", \"TestBS\".\"float8\" AS \"Float8\", \"TestBS\".\"bool\" AS \"Bool\", \"TestBS\".\"text\" AS \"Text\", \"TestBS\".\"time\" AS \"Time\" FROM \"preform_benchmark\".\"test_b\" AS \"TestBS\" WHERE (\"TestBS\".\"a_id\" IN ('??',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?))??", "$")
		if err != nil {
			b.Error(err)
		}
		if query != "SELECT \"TestBS\".\"id\" AS \"Id\", \"TestBS\".\"a_id\" AS \"AId\", \"TestBS\".\"name\" AS\"Name\", \"TestBS\".\"int4\" AS \"Int4\", \"TestBS\".\"int8\" AS \"Int8\", \"TestBS\".\"float4\"AS \"Float4\", \"TestBS\".\"float8\" AS \"Float8\", \"TestBS\".\"bool\" AS \"Bool\", \"TestBS\".\"text\" AS \"Text\", \"TestBS\".\"time\" AS \"Time\" FROM \"preform_benchmark\".\"test_b\" AS \"TestBS\" WHERE (\"TestBS\".\"a_id\" IN ('?',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,$84,$85,$86,$87,$88,$89,$90,$91,$92,$93,$94,$95,$96,$97,$98,$99,$100))?" {
			b.Error("unexpected result:" + query)
		}
	}
}

func BenchmarkReplacePositionalPlaceholdersFast(b *testing.B) {
	var (
		query string
		err   error
	)
	for i := 0; i < b.N; i++ {
		query, err = replacePositionalPlaceholdersFast("SELECT \"TestBS\".\"id\" AS \"Id\", \"TestBS\".\"a_id\" AS \"AId\", \"TestBS\".\"name\" AS\"Name\", \"TestBS\".\"int4\" AS \"Int4\", \"TestBS\".\"int8\" AS \"Int8\", \"TestBS\".\"float4\"AS \"Float4\", \"TestBS\".\"float8\" AS \"Float8\", \"TestBS\".\"bool\" AS \"Bool\", \"TestBS\".\"text\" AS \"Text\", \"TestBS\".\"time\" AS \"Time\" FROM \"preform_benchmark\".\"test_b\" AS \"TestBS\" WHERE (\"TestBS\".\"a_id\" IN ('??',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?))??", "$")
		if err != nil {
			b.Error(err)
		}
		if query != "SELECT \"TestBS\".\"id\" AS \"Id\", \"TestBS\".\"a_id\" AS \"AId\", \"TestBS\".\"name\" AS\"Name\", \"TestBS\".\"int4\" AS \"Int4\", \"TestBS\".\"int8\" AS \"Int8\", \"TestBS\".\"float4\"AS \"Float4\", \"TestBS\".\"float8\" AS \"Float8\", \"TestBS\".\"bool\" AS \"Bool\", \"TestBS\".\"text\" AS \"Text\", \"TestBS\".\"time\" AS \"Time\" FROM \"preform_benchmark\".\"test_b\" AS \"TestBS\" WHERE (\"TestBS\".\"a_id\" IN ('?',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,$84,$85,$86,$87,$88,$89,$90,$91,$92,$93,$94,$95,$96,$97,$98,$99,$100))?" {
			b.Error("unexpected result:" + query)
		}
	}
}
