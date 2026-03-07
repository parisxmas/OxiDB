package comparison_postgresql

import "os/exec"

func execCommand(name string, args ...string) *exec.Cmd {
	return exec.Command(name, args...)
}
