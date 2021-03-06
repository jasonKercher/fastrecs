package fastrecs

import "core:testing"

rec: Record 
reader: Reader 

@(private = "file")
_setup :: proc() {
	construct(&reader)
	rec = {}
}

@(private = "file")
_teardown :: proc() {
	destroy(&reader)
	rec = {}
}


@(test)
parse_rfc :: proc(t: ^testing.T) {
	_setup()

	ret: Status 

	ret = parse(&reader, &rec, "123,456,789,,")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = parse(&reader, &rec, "\"abc\",\"d,ef\",\"ghi\",\"\"")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d,ef")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = parse(&reader, &rec, "abc,\"de\nf\",ghi,")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = parse(&reader, &rec, "abc,\"de\"\"f\",ghi,")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"f")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	testing.expect_value(t, reader.rows, 4)

	set_delim(&reader, "~^_")
	ret = parse(&reader, &rec, "123~^_456~^_789~^_~^_")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = parse(&reader, &rec, "\"abc\"~^_\"d~^_ef\"~^_\"ghi\"~^_\"\"")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d~^_ef")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = parse(&reader, &rec, "abc~^_\"de\nf\"~^_ghi~^_")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = parse(&reader, &rec, "abc~^_\"de\"\"f\"~^_ghi~^_")
	testing.expect_value(t, ret, Status.Good)
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

	ret: Status 

	ret = parse(&reader, &rec, "123,456,789,,")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = parse(&reader, &rec, "\"abc\",\"d,ef\",\"ghi\",\"\"")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d,ef")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = parse(&reader, &rec, "abc,\"de\nf\",ghi,")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = parse(&reader, &rec, "abc,\"de\"\"f\",ghi,")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"\"f")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	testing.expect_value(t, reader.rows, 4)

	set_delim(&reader, "~^_")
	ret = parse(&reader, &rec, "123~^_456~^_789~^_~^_")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = parse(&reader, &rec, "\"abc\"~^_\"d~^_ef\"~^_\"ghi\"~^_\"\"")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d~^_ef")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = parse(&reader, &rec, "abc~^_\"de\nf\"~^_ghi~^_")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = parse(&reader, &rec, "abc~^_\"de\"\"f\"~^_ghi~^_")
	testing.expect_value(t, ret, Status.Good)
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

	ret: Status 

	ret = parse(&reader, &rec, "123,456,789,,")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = parse(&reader, &rec, "\"abc\",\"d,ef\",\"ghi\",\"\"")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "\"abc\"")
	testing.expect_value(t, rec.fields[1], "\"d")
	testing.expect_value(t, rec.fields[2], "ef\"")
	testing.expect_value(t, rec.fields[3], "\"ghi\"")
	testing.expect_value(t, rec.fields[4], "\"\"")

	ret = parse(&reader, &rec, "abc,\"de\nf\",ghi,")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de\nf\"")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = parse(&reader, &rec, "abc,\"de\"\"f\",ghi,")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de\"\"f\"")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	testing.expect_value(t, reader.rows, 4)

	set_delim(&reader, "~^_")
	ret = parse(&reader, &rec, "123~^_456~^_789~^_~^_")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")
	testing.expect_value(t, rec.fields[3], "")
	testing.expect_value(t, rec.fields[4], "")

	ret = parse(&reader, &rec, "\"abc\"~^_\"d~^_ef\"~^_\"ghi\"~^_\"\"")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 5)
	testing.expect_value(t, rec.fields[0], "\"abc\"")
	testing.expect_value(t, rec.fields[1], "\"d")
	testing.expect_value(t, rec.fields[2], "ef\"")
	testing.expect_value(t, rec.fields[3], "\"ghi\"")
	testing.expect_value(t, rec.fields[4], "\"\"")

	ret = parse(&reader, &rec, "abc~^_\"de\nf\"~^_ghi~^_")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de\nf\"")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	ret = parse(&reader, &rec, "abc~^_\"de\"\"f\"~^_ghi~^_")
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de\"\"f\"")
	testing.expect_value(t, rec.fields[2], "ghi")
	testing.expect_value(t, rec.fields[3], "")

	testing.expect_value(t, reader.rows, 8)

	_teardown()
}

@(private = "file")
_run_file_rfc :: proc(t: ^testing.T) {
	ret: Status 

	open(&reader, "basic.csv")
	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d|ef")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"f")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Eof)

	testing.expect_value(t, reader.rows, 4)
	testing.expect_value(t, reader.embedded_qty, 1)
}

@(test)
file_rfc :: proc(t: ^testing.T) {
	_setup()
	_run_file_rfc(t)
	_teardown()
}

@(test)
file_rfc_mmap :: proc(t: ^testing.T) {
	_setup()
	reader.config += {.Use_Mmap}
	_run_file_rfc(t)
	_teardown()
}

@(private = "file")
_run_file_weak :: proc(t: ^testing.T) {
	ret: Status 

	open(&reader, "basic.csv")
	reader.quote_style = .Weak
	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "d|ef")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\nf")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"\"f")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Eof)

	testing.expect_value(t, reader.rows, 4)
	testing.expect_value(t, reader.embedded_qty, 1)
}

@(test)
file_weak :: proc(t: ^testing.T) {
	_setup()
	_run_file_weak(t)
	_teardown()
}

@(test)
file_weak_mmap :: proc(t: ^testing.T) {
	_setup()
	reader.config += {.Use_Mmap}
	_run_file_weak(t)
	_teardown()
}

@(private = "file")
_run_file_none :: proc(t: ^testing.T) {
	ret: Status 

	open(&reader, "basic.csv")
	reader.quote_style = .None
	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "456")
	testing.expect_value(t, rec.fields[2], "789")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 4)
	testing.expect_value(t, rec.fields[0], "\"abc\"")
	testing.expect_value(t, rec.fields[1], "\"d")
	testing.expect_value(t, rec.fields[2], "ef\"")
	testing.expect_value(t, rec.fields[3], "\"ghi\"")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 2)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 2)
	testing.expect_value(t, rec.fields[0], "f\"")
	testing.expect_value(t, rec.fields[1], "ghi")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"de\"\"f\"")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Eof)

	testing.expect_value(t, reader.rows, 5)
	testing.expect_value(t, reader.embedded_qty, 0)
}

@(test)
file_none :: proc(t: ^testing.T) {
	_setup()
	_run_file_none(t)
	_teardown()
}

@(test)
file_none_mmap :: proc(t: ^testing.T) {
	_setup()
	reader.config += {.Use_Mmap}
	_run_file_none(t)
	_teardown()
}

@(private = "file")
_run_multi_eol_rfc :: proc(t: ^testing.T) {
	ret: Status

	open(&reader, "test_multi_eol.csv")
	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "4\n5\n6")
	testing.expect_value(t, rec.fields[2], "789")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Eof)

	testing.expect_value(t, reader.rows, 1)
	testing.expect_value(t, reader.embedded_qty, 2)
}

@(test)
multi_eol_rfc :: proc(t: ^testing.T) {
	_setup()
	_run_multi_eol_rfc(t)
	_teardown()
}

@(test)
multi_eol_rfc_mmap :: proc(t: ^testing.T) {
	_setup()
	reader.config += {.Use_Mmap}
	_run_multi_eol_rfc(t)
	_teardown()
}

@(private = "file")
_run_multi_eol_weak :: proc(t: ^testing.T) {
	reader.quote_style = .Weak

	ret: Status

	open(&reader, "test_multi_eol.csv")
	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "123")
	testing.expect_value(t, rec.fields[1], "4\n5\n6")
	testing.expect_value(t, rec.fields[2], "789")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Eof)

	testing.expect_value(t, reader.rows, 1)
	testing.expect_value(t, reader.embedded_qty, 2)
}

@(test)
multi_eol_weak :: proc(t: ^testing.T) {
	_setup()
	_run_multi_eol_weak(t)
	_teardown()
}

@(test)
multi_eol_weak_mmap :: proc(t: ^testing.T) {
	_setup()
	reader.config += {.Use_Mmap}
	_run_multi_eol_weak(t)
	_teardown()
}

@(private = "file")
_run_failsafe_eof :: proc(t: ^testing.T) {
	ret: Status

	reader.config += {.Failsafe}

	open(&reader, "test_fs_eof.csv")
	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Reset)

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Reset)

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, rec.fields[0], "\"")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, rec.fields[0], "")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Eof)
}

@(test)
failsafe_eof :: proc(t: ^testing.T) {
	_setup()
	_run_failsafe_eof(t)
	_teardown()
}

@(test)
failsafe_eof_mmap :: proc(t: ^testing.T) {
	_setup()
	reader.config += {.Use_Mmap}
	_run_failsafe_eof(t)
	_teardown()
}

@(private = "file")
_run_failsafe_max :: proc(t: ^testing.T) {
	ret: Status

	reader.config += {.Failsafe}

	open(&reader, "test_fs_max.csv")
	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Reset)

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Reset)

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "\"def")

	for ; ret == .Good; ret = get_record(&reader, &rec) {}

	testing.expect_value(t, ret, Status.Eof)
	testing.expect_value(t, reader.rows, 42)
}

@(test)
failsafe_max :: proc(t: ^testing.T) {
	_setup()
	_run_failsafe_max(t)
	_teardown()
}

@(test)
failsafe_max_mmap :: proc(t: ^testing.T) {
	_setup()
	reader.config += {.Use_Mmap}
	_run_failsafe_max(t)
	_teardown()
}

@(private = "file")
_run_failsafe_weak :: proc(t: ^testing.T) {
	ret: Status

	reader.config += {.Failsafe}

	open(&reader, "test_fs_weak.csv")
	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Reset)

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Good)
	testing.expect_value(t, len(rec.fields), 3)
	testing.expect_value(t, rec.fields[0], "abc")
	testing.expect_value(t, rec.fields[1], "de\"f")
	testing.expect_value(t, rec.fields[2], "ghi")

	ret = get_record(&reader, &rec)
	testing.expect_value(t, ret, Status.Eof)
}

@(test)
failsafe_weak :: proc(t: ^testing.T) {
	_setup()
	_run_failsafe_weak(t)
	_teardown()
}

@(test)
failsafe_weak_mmap :: proc(t: ^testing.T) {
	_setup()
	reader.config += {.Use_Mmap}
	_run_failsafe_weak(t)
	_teardown()
}
