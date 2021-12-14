package csvtool

import "core:testing"
import "fastrecs"

rec: fastrecs.Record 
reader: fastrecs.Reader 

@(private = "file")
_setup :: proc() {
	fastrecs.construct(&reader)
	rec = {}
}

@(private = "file")
_teardown :: proc() {
	fastrecs.destroy(&reader)
	rec = {}
}


@(test)
parse_rfc :: proc(t: ^testing.T) {
	_setup()

	ret: fastrecs.Status 

	ret = fastrecs.parse(&reader, &rec, "123,456,789,,")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = fastrecs.parse(&reader, &rec, "\"abc\",\"d,ef\",\"ghi\",\"\"")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d,ef")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = fastrecs.parse(&reader, &rec, "abc,\"de\nf\",ghi,")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = fastrecs.parse(&reader, &rec, "abc,\"de\"\"f\",ghi,")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"f")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	testing.expect_value(t, reader.rows, 4)

	fastrecs.set_delim(&reader, "~^_")
	ret = fastrecs.parse(&reader, &rec, "123~^_456~^_789~^_~^_")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = fastrecs.parse(&reader, &rec, "\"abc\"~^_\"d~^_ef\"~^_\"ghi\"~^_\"\"")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d~^_ef")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = fastrecs.parse(&reader, &rec, "abc~^_\"de\nf\"~^_ghi~^_")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = fastrecs.parse(&reader, &rec, "abc~^_\"de\"\"f\"~^_ghi~^_")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"f")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	testing.expect_value(t, reader.rows, 8)

	_teardown()
}

@(test)
parse_weak :: proc(t: ^testing.T) {
	_setup()
	reader.quote_style = .Weak

	ret: fastrecs.Status 

	ret = fastrecs.parse(&reader, &rec, "123,456,789,,")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = fastrecs.parse(&reader, &rec, "\"abc\",\"d,ef\",\"ghi\",\"\"")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d,ef")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = fastrecs.parse(&reader, &rec, "abc,\"de\nf\",ghi,")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = fastrecs.parse(&reader, &rec, "abc,\"de\"\"f\",ghi,")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"\"f")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	testing.expect_value(t, reader.rows, 4)

	fastrecs.set_delim(&reader, "~^_")
	ret = fastrecs.parse(&reader, &rec, "123~^_456~^_789~^_~^_")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = fastrecs.parse(&reader, &rec, "\"abc\"~^_\"d~^_ef\"~^_\"ghi\"~^_\"\"")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d~^_ef")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = fastrecs.parse(&reader, &rec, "abc~^_\"de\nf\"~^_ghi~^_")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = fastrecs.parse(&reader, &rec, "abc~^_\"de\"\"f\"~^_ghi~^_")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"\"f")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	testing.expect_value(t, reader.rows, 8)

	_teardown()
}

@(test)
parse_none :: proc(t: ^testing.T) {
	_setup()
	reader.quote_style = .None

	ret: fastrecs.Status 

	ret = fastrecs.parse(&reader, &rec, "123,456,789,,")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = fastrecs.parse(&reader, &rec, "\"abc\",\"d,ef\",\"ghi\",\"\"")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "\"abc\"")
	testing.expect_value(t, rec.fields[1], "\"d")
	testing.expect_value(t, rec.fields[2], "ef\"")
	testing.expect_value(t, rec.fields[3], "\"ghi\"")
	testing.expect_value(t, rec.fields[4], "\"\"")

	ret = fastrecs.parse(&reader, &rec, "abc,\"de\nf\",ghi,")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de\nf\"")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = fastrecs.parse(&reader, &rec, "abc,\"de\"\"f\",ghi,")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de\"\"f\"")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	testing.expect_value(t, reader.rows, 4)

	fastrecs.set_delim(&reader, "~^_")
	ret = fastrecs.parse(&reader, &rec, "123~^_456~^_789~^_~^_")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = fastrecs.parse(&reader, &rec, "\"abc\"~^_\"d~^_ef\"~^_\"ghi\"~^_\"\"")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "\"abc\"")
	testing.expect_value(t, rec.fields[1], "\"d")
	testing.expect_value(t, rec.fields[2], "ef\"")
	testing.expect_value(t, rec.fields[3], "\"ghi\"")
	testing.expect_value(t, rec.fields[4], "\"\"")

	ret = fastrecs.parse(&reader, &rec, "abc~^_\"de\nf\"~^_ghi~^_")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de\nf\"")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = fastrecs.parse(&reader, &rec, "abc~^_\"de\"\"f\"~^_ghi~^_")
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de\"\"f\"")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	testing.expect_value(t, reader.rows, 8)

	_teardown()
}

@(test)
file_rfc :: proc(t: ^testing.T) {
	_setup()

	ret: fastrecs.Status 

	fastrecs.open(&reader, "basic.csv")
	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d|ef")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"f")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Eof)

	testing.expect_value(t, reader.rows, 4)
	testing.expect_value(t, reader.embedded_qty, 1)

	_teardown()
}

@(test)
file_weak :: proc(t: ^testing.T) {
	_setup()

	ret: fastrecs.Status 

	fastrecs.open(&reader, "basic.csv")
	reader.quote_style = .Weak
	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d|ef")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"\"f")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Eof)

	testing.expect_value(t, reader.rows, 4)
	testing.expect_value(t, reader.embedded_qty, 1)

	_teardown()
}

@(test)
file_none :: proc(t: ^testing.T) {
	_setup()

	ret: fastrecs.Status 

	fastrecs.open(&reader, "basic.csv")
	reader.quote_style = .None
	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "\"abc\"")
	testing.expect_value(t, rec.fields[1], "\"d")
	testing.expect_value(t, rec.fields[2], "ef\"")
	testing.expect_value(t, rec.fields[3], "\"ghi\"")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 2)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 2)
	testing.expect_value(t, rec.fields[0], "f\"")
	testing.expect_value(t, rec.fields[1], "ghi")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de\"\"f\"")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Eof)

	testing.expect_value(t, reader.rows, 5)
	testing.expect_value(t, reader.embedded_qty, 0)

	_teardown()
}

@(test)
multi_eol :: proc(t: ^testing.T) {
	_setup()

	ret: fastrecs.Status

	fastrecs.open(&reader, "test_multi_eol.csv")
	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "4\n5\n6")
	testing.expect_value(t, rec.fields[2], "789")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Eof)

	testing.expect_value(t, reader.rows, 1)
	testing.expect_value(t, reader.embedded_qty, 2)

	_teardown()
}

//@(test)
failsafe_eof :: proc(t: ^testing.T) {
	_setup()

	ret: fastrecs.Status

	reader.config += {.Failsafe}

	fastrecs.open(&reader, "test_fs_eof.csv")
	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Reset)

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Reset)

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, rec.fields[0], "\"")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Eof)
	
	_teardown()
}

//@(test)
failsafe_max :: proc(t: ^testing.T) {
	_setup()

	ret: fastrecs.Status

	reader.config += {.Failsafe}

	fastrecs.open(&reader, "test_fs_max.csv")
	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Reset)

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Reset)

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"def")

	for ; ret == .Good; ret = fastrecs.get_record(&reader, &rec) {}

	testing.expect_value(t, ret, fastrecs.Status.Eof)
	testing.expect_value(t, reader.rows, 41)
	
	_teardown()
}

//@(test)
failsafe_weak :: proc(t: ^testing.T) {
	_setup()

	ret: fastrecs.Status

	reader.config += {.Failsafe}

	fastrecs.open(&reader, "test_fs_weak.csv")
	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Reset)

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Good)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"f")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = fastrecs.get_record(&reader, &rec)
	testing.expect_value(t, ret, fastrecs.Status.Eof)
	
	_teardown()
}
