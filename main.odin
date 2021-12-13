package csvtool

import "core:bufio"
import "core:fmt"
import "core:os"
import "core:io"
import "getargs"
import "fastrecs"

main :: proc() {
	argparser := getargs.make_getargs()
	getargs.add_arg(&argparser, "m", "mmap", getargs.Optarg_Option.None)
	getargs.read_args(&argparser, os.args)

	stdout_writer, ok := io.to_writer(os.stream_from_handle(os.stdout))
	if !ok {
		os.write_string(os.stderr, "failed to build io.writer\n")
		os.exit(2)
	}
	buf_stdout : bufio.Writer
	bufio.writer_init(&buf_stdout, stdout_writer)

	rec: fastrecs.Record
	reader: fastrecs.Reader

	cfg : bit_set[fastrecs.Config]
	if getargs.get_flag(&argparser, "m") {
		cfg += {.Use_Mmap}
	}

	fastrecs.reader_construct(&reader, cfg)

	if argparser.arg_idx < len(os.args) {
		fastrecs.open(&reader, os.args[argparser.arg_idx])
	} else {
		fastrecs.open(&reader)
	}

	loop: for {
		e := fastrecs.get_record(&reader, &rec)
		#partial switch e {
		case .Eof:
			break loop
		case .Error:
			os.exit(3)
		}

		first := true
		for f in rec.fields {
			if !first {
				bufio.writer_write_byte(&buf_stdout, ',')
			}
			first = false
			bufio.writer_write_string(&buf_stdout, f)
		}
		bufio.writer_write_byte(&buf_stdout, '\n')
	}

	fastrecs.close(&reader)
	fastrecs.reader_destroy(&reader)

	bufio.writer_flush(&buf_stdout)

	getargs.destroy(&argparser)
}
