package fastrecs

import "core:intrinsics"
import "core:sys/unix"

/* Currently, there is a PR to put these into core/sys/unix/syscall_linux.odin
 * Until that is merged, keep these here.
 */


sys_mmap :: proc "contextless" (addr: rawptr, length: uint, prot: int, flags: int, fd: i32, offset: uint) -> rawptr {
	res := intrinsics.syscall(unix.SYS_mmap, uintptr(addr), uintptr(length), uintptr(prot), uintptr(flags), uintptr(fd), uintptr(offset))
	return rawptr(res)
}

sys_munmap :: proc "contextless" (addr: rawptr, length: uint) -> int {
	return int(intrinsics.syscall(unix.SYS_munmap, uintptr(addr), uintptr(length)))
}

sys_mprotect :: proc "contextless" (addr: rawptr, length: uint, prot: int) -> int {
	return int(intrinsics.syscall(unix.SYS_mprotect, uintptr(addr), uintptr(length), uint(prot)))
}

sys_madvise :: proc "contextless" (addr: rawptr, length: uint, advice: int) -> int {
	return int(intrinsics.syscall(unix.SYS_madvise, uintptr(addr), uintptr(length), uintptr(advice)))
}
