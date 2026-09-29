package sim

import (
	"fmt"
	"html"
	"os"
	"strings"
)

func WriteTimeline(path string, log *Log, title string) error {
	var b strings.Builder
	b.WriteString("<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>")
	b.WriteString(html.EscapeString(title))
	b.WriteString(`</title><style>
body{font-family:ui-monospace,monospace;background:#0b1020;color:#d7e0ff;margin:24px}
h1{font-size:18px}
.e{white-space:pre;border-bottom:1px solid #1c2744;padding:2px 0}
.violate{color:#ff8080}
.deliver{color:#9ad1ff}
.state{color:#b6f2c5}
.client{color:#ffd37a}
</style></head><body><h1>`)
	b.WriteString(html.EscapeString(title))
	b.WriteString("</h1>\n")
	for _, e := range log.Events() {
		cls := "e"
		switch e.Kind {
		case EvViolate:
			cls = "e violate"
		case EvDeliver:
			cls = "e deliver"
		case EvState:
			cls = "e state"
		case EvClient:
			cls = "e client"
		}
		fmt.Fprintf(&b, "<div class=\"%s\">%s</div>\n", cls, html.EscapeString(e.Canonical()))
	}
	b.WriteString("</body></html>")
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
