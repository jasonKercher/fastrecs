package fastrecs

import "core:mem/virtual"
import "core:math/bits"
import "core:strings"
import "core:bufio"
import "core:bytes"
import "core:fmt"
import "core:mem"
import "core:io"
import "core:os"
import "core:c"


MAX_EMBEDDED_NEW_LINES :: 8


Quotes :: enum {
	None,
	Weak,
	Rfc4180,
	All,
}

Error :: enum {
	None,
	Error,
	Reset,
	Eof,
}

Config :: enum {
	Failsafe,
	Trim,
	Use_Mmap,
	_Using_Mmap,
	_From_Get_Record,
	_File_Open,
}

Record :: struct {
	fields: []string,
	_b:     i32, /* _builders index */
	_f:     i32, /* _fields index */
}

Reader :: struct {
	file:           os.Handle,
	file_size:      u64,
	_delim:         string,
	_weak_delim:    string,
	embedded_break: string,
	offset:         u64,
	rows:           u32,
	embedded_qty:   u32,
	_mmap_ptr:      [^]u8,
	_normal:        i32,
	_normal_org:    i32,
	_reader:        bufio.Reader,
	_builders:      [dynamic]strings.Builder,
	_line_buffers:  [dynamic][dynamic]u8,
	_fields:        [dynamic][dynamic]string,
	config:         bit_set[Config],
	quote_style:    Quotes,
}

reader_construct :: proc(self: ^Reader, cfg: bit_set[Config] = {}) {
	self^ =  {
		file           = os.stdin,
		_builders      = make([dynamic]strings.Builder),
		_fields        = make([dynamic][dynamic]string),
		_line_buffers  = make([dynamic][dynamic]u8),
		quote_style    = .Rfc4180,
		_normal        = -1,
		_normal_org    = -1,
		embedded_break = "\n",
		config         = cfg,
	}
}

reader_destroy :: proc(self: ^Reader) {
	close(self)

	if self._weak_delim != "" {
		delete(self._weak_delim)
	}
	for i := 0; i < len(self._builders); i += 1 {
		strings.destroy_builder(&self._builders[i])
	}
	delete(self._builders)
	delete(self._fields)

	for lb in self._line_buffers {
		delete(lb)
	}
	delete(self._line_buffers)
}

/* Send blank string (or nothing) to build reader for stdin */
open :: proc(self: ^Reader, file_name: string = "") -> Error {
	close(self)

	if file_name != "" {
		errno: os.Errno 
		self.file, errno = os.open(file_name, os.O_RDONLY, 0)
		if errno != 0 {
			return _error("file error")
		}
		self.config +=  {
			._File_Open,
		}

		size: i64 
		size, errno = os.file_size(self.file)
		if errno != 0 {
			return _error("file_size error")
		}
		if size == 0 {
			return nil
		}
		self.file_size = u64(size)

		if .Use_Mmap in self.config {
			using virtual
			m := mmap(nil, uint(size), PROT_READ, MAP_PRIVATE, i32(self.file), 0)
			if m == nil {
				return _error("mmap failed")
			}
			madvise(m, uint(size), MADV_SEQUENTIAL)

			self._mmap_ptr = ([^]u8)(m)
			self.config +=  {
				._Using_Mmap,
			}
			return nil
		}
	}

	self.config +=  {
		._File_Open,
	}
	self.config -=  {
		._Using_Mmap,
	}

	r, ok := io.to_reader(os.stream_from_handle(self.file))
	bufio.reader_init(&self._reader, r)

	return nil
}

close :: proc(self: ^Reader) -> Error {
	if ._File_Open not_in self.config {
		return nil
	}

	if ._Using_Mmap in self.config {
		virtual.munmap(rawptr(self._mmap_ptr), uint(self.file_size))
		self._mmap_ptr = nil
		self.config -=  {
			._Using_Mmap,
		}
	} else {
		bufio.reader_destroy(&self._reader)
	}

	if self.file != os.stdin {
		os.close(self.file)
		self.file = os.stdin
	}
	return nil
}

seek :: proc(self: ^Reader, offset: u64) -> Error {
	if offset > self.file_size {
		return .Error
	}
	self.offset = offset

	if ._Using_Mmap in self.config {
		return nil /* lol */
	}

	os.seek(self.file, i64(offset), os.SEEK_SET)
	return nil
}

/* probably pick something out of mem/virtual */
advise :: proc(self: ^Reader, advise: c.int) -> Error {
	if ._Using_Mmap not_in self.config {
		return nil
	}
	if virtual.madvise(self._mmap_ptr, uint(self.file_size), advise) != 0 {
		return _error("madvise")
	}
	return nil
}

reset :: proc(self: ^Reader) -> Error {
	self.rows = 0
	self.embedded_qty = 0
	self._normal = self._normal_org
	return seek(self, 0)
}

set_normal :: proc(self: ^Reader, normal: int) {
	self._normal_org = i32(normal)
	self._normal = i32(normal)
}

set_delim :: proc(self: ^Reader, delim: string) -> Error {
	self._delim = delim
	if len(self._weak_delim) != 0 {
		delete(self._weak_delim)
	}
	fmt.aprintf(self._weak_delim, "\"%s", delim)

	return nil
}

/* The full record string is located one string behind rec.fields[0].
 * Convert to pointer and move back one to retrive.
 */
get_line_from_record :: proc(rec: Record) -> string {
	s_ptr := mem.raw_data(rec.fields)
	s_ptr = mem.ptr_offset(s_ptr, -1)
	return s_ptr^
}

get_record :: proc(
	self: ^Reader,
	rec: ^Record,
	field_limit: int = bits.I32_MAX,
) -> Error {
	rec_str: string 

	if rec._f == 0 {
		_init_record(self, rec)
	}

	if ._Using_Mmap in self.config {
		_get_line_mmap(self, &rec_str) or_return
	} else {
		_get_line(self, rec, &rec_str) or_return
	}

	self.config +=  {
		._From_Get_Record,
	}
	ret := parse(self, rec, rec_str, len(rec_str), field_limit)
	self.config -=  {
		._From_Get_Record,
	}

	return ret
}

parse :: proc(
	self: ^Reader,
	rec: ^Record,
	rec_str: string,
	byte_limit: int = bits.I32_MAX,
	field_limit: int = bits.I32_MAX,
) -> Error {
	field_limit := field_limit
	byte_limit := byte_limit

	if len(self._delim) == 0 {
		_find_delim(self, rec_str)
	}

	if rec._f == 0 {
		_init_record(self, rec)
	}
	fields : ^[dynamic]string = _get_fields(self, rec)
	clear(fields)
	append(fields, rec_str)

	if self._normal > 0 {
		field_limit = int(self._normal)
	}

	rec_idx: int 

	for rec_idx < byte_limit && len(fields^) < field_limit {
		if len(fields^) > 1 {
			rec_idx += len(self._delim)
		}

		quotes := self.quote_style
		if quotes != .None && rec_str[rec_idx] != '"' {
			quotes = .None
		}

		e: Error 
		switch quotes {
		case .All:
			fallthrough
		case .Rfc4180:
			e = _parse_rfc4180(self, rec, &rec_idx, &byte_limit)
		case .Weak:
			e = _parse_weak(self, rec, &rec_idx, &byte_limit)
		case .None:
			e = _parse_none(self, rec, &rec_idx, &byte_limit)
		}

		if e == .Reset {
			e = _lower_standard(self)
			reset(self)
			return e
		}

	}

	if self._normal == 0 {
		self._normal = i32(len(fields^))
	}
	self.rows += 1
	rec.fields = fields[1:]

	return nil
}

@(private = "file")
_init_record :: proc(self: ^Reader, rec: ^Record) {
	rec^ =  {
		_b = -1,
		_f = i32(len(self._fields)) + 1,
	}
	append_nothing(&self._fields)
	append_nothing(_get_fields(self, rec))
	if ._Using_Mmap not_in self.config {
		append_nothing(&self._line_buffers)
	}
}

@(private = "file")
_get_builder :: proc(self: ^Reader, rec: ^Record) -> strings.Builder {
	if rec._b == -1 {
		rec._b = i32(len(self._builders))
		append(&self._builders, strings.make_builder())
	}
	return self._builders[rec._b]
}

@(private = "file")
_get_line_buffer :: proc(self: ^Reader, rec: ^Record) -> ^[dynamic]u8 {
	return &self._line_buffers[rec._f - 1]
}

@(private = "file")
_get_fields :: proc(self: ^Reader, rec: ^Record) -> ^[dynamic]string {
	return &self._fields[rec._f - 1]
}

@(private = "file")
_parse_rfc4180 :: proc(self: ^Reader, rec: ^Record, rec_idx, byte_limit: ^int) -> Error {
	keep := true
	first_char := true
	last_was_quote := false
	qualified := true
	delim_skip := false

	rec_idx^ += 1
	begin := rec_idx^
	iter := begin
	nl_count: u32 
	end: int 

	field_builder := _get_builder(self, rec)

	fields : ^[dynamic]string = _get_fields(self, rec)
	rec_str := fields[0]

	append_nothing(fields)
	field_str := fields[len(fields^) - 1]

	for {
		for ; qualified && end != byte_limit^; iter = end + len(self._delim) {
			end = strings.index(rec_str[iter:], self._delim)

			if !delim_skip && iter != begin {
				strings.write_string(&field_builder, self._delim)
			}

			delim_skip = (end == -1)
			if delim_skip {
				end = byte_limit^ + begin
			}

			strings.grow_builder(&field_builder, end - begin)

			for i := iter; i < end; i += 1 {
				keep = true
				if qualified {
					qualified = rec_str[i] != '"'
					if !qualified {
						keep = false
						last_was_quote = true
					}
				} else {
					qualified = rec_str[i] == '"'
					if qualified && !last_was_quote {
						keep = false
					}
					last_was_quote = false
				}
				/* ltrim */
				if !keep || (first_char && .Trim in self.config && strings.is_space(
					   rune(rec_str[i]),
				   )) {
					continue
				}
				strings.write_byte(&field_builder, rec_str[i])
				first_char = false
			}
		}
		if !qualified {
			break
		}

		nl_count += 1

		if ._From_Get_Record in self.config || nl_count > MAX_EMBEDDED_NEW_LINES {
			return .Reset
		}

		ret: Error 
		if ._Using_Mmap in self.config {
			ret = _get_line_mmap(self, &rec_str)
		} else {
			ret = _get_line(self, rec, &rec_str)
		}
		if ret == .Eof {
			return .Reset
		}
		strings.write_string(&field_builder, self.embedded_break)
		byte_limit^ = len(rec_str)
	}

	if .Trim in self.config {
		/* rtrim */
		append(fields, strings.trim_right_space(strings.to_string(field_builder)))
	} else {
		append(fields, strings.to_string(field_builder))
	}

	rec_idx^ += end
	self.embedded_qty += nl_count

	return nil
}

@(private = "file")
_parse_weak :: proc(self: ^Reader, rec: ^Record, rec_idx, byte_limit: ^int) -> Error {
	nl_count: u32 

	rec_idx^ += 1
	begin := rec_idx^
	field_builder := _get_builder(self, rec)

	fields : ^[dynamic]string = _get_fields(self, rec)
	rec_str := fields[0]

	append_nothing(fields)
	field_str := fields[len(fields^) - 1]

	end := strings.index(rec_str[begin:], self._weak_delim)
	for end == -1 {
		end = byte_limit^ + begin

		/* quote before EOL */
		if rec_str[end - 1] == '"' && begin != end {
			end -= 1
			break
		}

		if ._From_Get_Record in self.config || nl_count > MAX_EMBEDDED_NEW_LINES {
			return .Reset
		}

		ret: Error 
		if ._Using_Mmap in self.config {
			ret = _get_line_mmap(self, &rec_str)
		} else {
			ret = _get_line(self, rec, &rec_str)
		}
		if ret == .Eof {
			return .Reset
		}

		old_limit := byte_limit^
		end = strings.index(rec_str[begin + old_limit:], self._weak_delim)
	}

	strings.grow_builder(&field_builder, end - begin)

	if .Trim in self.config {
		i := begin
		for ; i < end && strings.is_space(rune(rec_str[i])); i += 1 {}
		strings.write_string(&field_builder, rec_str[i:i+end])
		append(fields, strings.trim_right_space(strings.to_string(field_builder)))
	} else {
		strings.write_string(&field_builder, rec_str[begin:begin+end])
		append(fields, strings.to_string(field_builder))
	}

	rec_idx^ += end + 1

	self.embedded_qty += nl_count

	return nil
}

@(private = "file")
_parse_none :: proc(self: ^Reader, rec: ^Record, rec_idx, byte_limit: ^int) -> Error {
	begin := rec_idx^
	nl_count: u32 

	fields : ^[dynamic]string = _get_fields(self, rec)
	rec_str := fields[0]

	end := strings.index(rec_str[begin:], self._delim)
	if end == -1 {
		end = byte_limit^ - begin
	}

	if .Trim in self.config {
		i := begin
		for ; i < end && strings.is_space(rune(rec_str[i])); i += 1 {}
		append(fields, strings.trim_right_space(rec_str[i:i+end]))
	} else {
		append(fields, rec_str[begin:begin+end])
	}

	rec_idx^ += end

	return nil
}

@(private = "file")
_find_delim :: proc(self: ^Reader, rec_str: string) {
	char_count :: proc(rec_str: string, delim: u8) -> int {
		n: int 
		for c in rec_str {
			if c == rune(delim) {
				n += 1
			}
		}
		return n
	}

	max_count: int 
	delims := ",\t|;:"

	max_count = char_count(rec_str, ',')

	max_idx := 0
	i := 1

	for ; i < len(delims); i += 1 {
		count := char_count(rec_str, delims[i])
		if count > max_count {
			max_count = count
			max_idx = i
		}
	}

	set_delim(self, delims[max_idx:max_idx+1])
}

@(private = "file")
_get_line_mmap :: proc(self: ^Reader, rec_str: ^string) -> Error {
	if self._mmap_ptr == nil || self.offset >= self.file_size {
		return .Eof
	}

	start_len := len(rec_str^)
	start_offset := self.offset - u64(start_len)

	eol := bytes.index_byte(self._mmap_ptr[self.offset:self.file_size], '\n')
	if eol == -1 {
		rec_str^ = string(self._mmap_ptr[start_offset:self.file_size])
	} else if self._mmap_ptr[self.offset + u64(eol) - 1] == '\r' {
		rec_str^ = string(self._mmap_ptr[start_offset:self.offset + u64(eol) - 1])
	} else {
		rec_str^ = string(self._mmap_ptr[start_offset:self.offset + u64(eol)])
	}

	self.offset += u64(eol) + 1

	if self.offset >= self.file_size && len(rec_str^) == start_len {
		return .Eof
	}

	return nil
}

@(private = "file")
_get_line :: proc(self: ^Reader, rec: ^Record, rec_str: ^string) -> Error {
	buf := _get_line_buffer(self, rec)
	if len(rec_str^) == 0 {
		clear(buf)
	}

	e := io.Error.Buffer_Full
	line: []u8 

	loop: for e == .Buffer_Full {
		line, e = bufio.reader_read_slice(&self._reader, '\n')
		append(buf, ..line)
	}
	if e != nil && e != .No_Progress {
		return .Error
	}

	rec_str^ = string(buf[:])
	length := len(rec_str^)

	if e == .EOF || e == .No_Progress {
		return .Eof
	}

	if length >= 2 && rec_str^[length - 2] == '\r' {
		rec_str^ = rec_str[:length - 2]
	} else if e == nil {
		rec_str^ = rec_str[:length - 1]
	}

	return nil
}

@(private = "file")
_lower_standard :: proc(self: ^Reader) -> Error {
	if .Failsafe in self.config || self.file == os.stdin {
		return _error("qualifier error")
	}

	switch self.quote_style {
	case .All:
		fallthrough
	case .Rfc4180:
		fmt.fprintf(os.stderr, "qualifier error. attempting to use `Weak' quote_style\n")
		self.quote_style = .Weak
	case .Weak:
		fmt.fprintf(os.stderr, "qualifier error. attempting to use `None' quote_style\n")
		self.quote_style = .None
	case .None:
		return _error("parsing error")
	}

	reset(self)
	return nil
}

@(private = "file")
_error :: proc(msg: string) -> Error {
	fmt.fprintf(os.stderr, "%s\n", msg)
	return .Error
}
