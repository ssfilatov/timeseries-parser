package errorx

import "fmt"

type genericError struct {
	msg string
}

func (e genericError) Error() string {
	return e.msg
}

func BadRequest(err error) error {
	return WrapWithMessage(err, "error decoding request body")
}

func WrapWithMessage(e error, msg string) error {
	return genericError{
		msg: fmt.Sprintf("%s, %s", msg, e),
	}
}

func New(msg string) error {
	return genericError{
		msg: msg,
	}
}
