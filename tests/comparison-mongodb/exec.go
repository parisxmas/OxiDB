package comparison_mongodb

import "os/exec"

func execCommand(name string, args ...string) *exec.Cmd {
	return exec.Command(name, args...)
}
