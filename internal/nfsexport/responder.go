package nfsexport

import (
	"fmt"
)

// Responder answers a single cache-channel request against an ExportTable. It
// is deliberately I/O-free: callers pass an already-read request line and get
// back the answer line to write. The poll/read loop over the actual
// /proc/net/rpc/<name>/channel files is layered on top in a later slice, which
// keeps this core hermetically testable.
type Responder struct {
	table ExportTable
}

// NewResponder builds a Responder backed by the given table.
func NewResponder(table ExportTable) *Responder {
	return &Responder{table: table}
}

// Answer decodes one request line for the given channel and returns the answer
// line to write back. A miss (no matching export) yields a NEGATIVE answer,
// which is the correct kernel signal to reject the mount rather than defer
// forever. It returns an error only when the request line itself is malformed.
func (r *Responder) Answer(ch Channel, line string) (string, error) {
	toks, err := parseQwords(line)
	if err != nil {
		return "", err
	}
	if len(toks) == 0 {
		return "", fmt.Errorf("%w: empty request line", ErrMalformedQword)
	}
	switch ch {
	case ChannelAuthUnixIP:
		return r.answerAuthUnixIP(toks)
	case ChannelExpKey:
		return r.answerExpKey(toks)
	case ChannelExport:
		return r.answerExport(toks)
	default:
		return "", fmt.Errorf("nfsexport: unknown channel %d", ch)
	}
}

func (r *Responder) answerAuthUnixIP(toks []token) (string, error) {
	req, err := parseIPMapRequest(toks)
	if err != nil {
		return "", err
	}
	_, entry, ok := r.table.LookupClient(req.Addr)
	exp := expiryUnix()
	if !ok {
		// No export permits this client: NEGATIVE (empty domain).
		return marshalIPMapAnswer(req.Class, req.Addr, exp, "", true), nil
	}
	return marshalIPMapAnswer(req.Class, req.Addr, exp, entry.DomainName(), false), nil
}

func (r *Responder) answerExpKey(toks []token) (string, error) {
	req, err := parseExpKeyRequest(toks)
	if err != nil {
		return "", err
	}
	if req.Domain != "*" {
		return marshalExpKeyAnswer(req.Domain, req.FSIDType, req.FSID, expiryUnix(), "", true), nil
	}
	entry, ok := r.table.LookupPath(req.FSIDType, req.FSID)
	exp := expiryUnix()
	if !ok {
		return marshalExpKeyAnswer(req.Domain, req.FSIDType, req.FSID, exp, "", true), nil
	}
	return marshalExpKeyAnswer("*", req.FSIDType, req.FSID, exp, entry.Path, false), nil
}

func (r *Responder) answerExport(toks []token) (string, error) {
	req, err := parseExportRequest(toks)
	if err != nil {
		return "", err
	}
	if req.Domain != "*" {
		return marshalExportAnswer(Entry{}, req.Domain, req.Path, expiryUnix(), true), nil
	}
	entry, ok := r.table.LookupExport(req.Domain, req.Path)
	exp := expiryUnix()
	if !ok {
		return marshalExportAnswer(Entry{}, req.Domain, req.Path, exp, true), nil
	}
	return marshalExportAnswer(entry, "*", req.Path, exp, false), nil
}
