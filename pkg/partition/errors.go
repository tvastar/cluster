// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

package partition

type errors []error

func (e *errors) check(err error) {
	if err != nil {
		*e = append(*e, err)
	}
}

func (e *errors) toError() error {
	if len(*e) == 0 {
		return nil
	}
	return (*e)[0]
}

// IncorrectPartitionError is a transient error that happens when
// requests end up on the wrong partition.
type IncorrectPartitionError struct{}

// Error returns the error string
func (e IncorrectPartitionError) Error() string {
	return "incorrect partition, retry later"
}
