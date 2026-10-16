package nats

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ProvisionMode controls how Start syncs a declared stream or consumer with
// the server. Set the App's default with WithProvision and override it per
// declaration in Stream or Consumer.
//
// CreateOnly and Verify compare only the fields set in the declaration: a
// zero field means "any value", because the server fills its own defaults
// there. Map fields such as Metadata only need to contain the declared keys.
// So a declaration cannot require a field to be zero, and the zero value is
// also the default of FileStorage, LimitsPolicy, DiscardOld, AckExplicitPolicy
// and DeliverAllPolicy: declaring those does not detect a server set to
// MemoryStorage, WorkQueuePolicy and so on. Fields the server overrides are
// skipped (a consumer's AckWait when BackOff is set, StreamSource.Domain); a
// value the server cannot apply, such as MirrorDirect on a mirror whose origin
// has no AllowDirect, is reported as drift.
type ProvisionMode int

const (
	// Upsert creates the stream or consumer, or updates it to the declared
	// config. A change the server rejects (such as an immutable field) fails
	// Start. It is the default.
	Upsert ProvisionMode = iota + 1
	// CreateOnly creates the stream or consumer if it is missing and never
	// updates an existing one; a config that differs from the declaration is
	// logged at Warn ("nats: config drift") and left as is.
	CreateOnly
	// Verify never writes: Start fails if the stream or consumer is missing,
	// or with ErrConfigDrift if its config differs from the declaration. Use
	// it for streams another service owns.
	Verify
)

func (m ProvisionMode) valid() bool { return m >= Upsert && m <= Verify }

type streamDecl struct {
	cfg  jetstream.StreamConfig
	mode ProvisionMode
}

type consumerDecl struct {
	stream string
	cfg    jetstream.ConsumerConfig
	mode   ProvisionMode
}

func (d consumerDecl) name() string {
	if d.cfg.Durable != "" {
		return d.cfg.Durable
	}
	return d.cfg.Name
}

// declMode validates the optional per-declaration mode; 0 means the App's.
func declMode(what string, mode []ProvisionMode) (ProvisionMode, error) {
	switch {
	case len(mode) == 0:
		return 0, nil
	case len(mode) > 1:
		return 0, fmt.Errorf("nats: %s: more than one ProvisionMode", what)
	case !mode[0].valid():
		return 0, fmt.Errorf("nats: %s: invalid ProvisionMode %d", what, mode[0])
	}
	return mode[0], nil
}

// Stream declares a stream that Start syncs according to its ProvisionMode:
// the optional mode, else the App's WithProvision, else Upsert. Streams are
// never deleted.
func (a *App) Stream(cfg jetstream.StreamConfig, mode ...ProvisionMode) {
	if cfg.Name == "" {
		a.addErr(errors.New("nats: stream: empty name"))
		return
	}
	m, err := declMode(fmt.Sprintf("stream %q", cfg.Name), mode)
	if err != nil {
		a.addErr(err)
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lateLocked() {
		return
	}
	a.streams = append(a.streams, streamDecl{cfg: cfg, mode: m})
}

// Consumer declares a durable consumer that Start syncs after the streams,
// according to its ProvisionMode like Stream. A config with DeliverSubject
// is a push consumer.
func (a *App) Consumer(stream string, cfg jetstream.ConsumerConfig, mode ...ProvisionMode) {
	d := consumerDecl{stream: stream, cfg: cfg}
	if stream == "" || d.name() == "" {
		a.addErr(errors.New("nats: consumer: stream and Durable (or Name) are required"))
		return
	}
	m, err := declMode(fmt.Sprintf("consumer %q", d.name()), mode)
	if err != nil {
		a.addErr(err)
		return
	}
	d.mode = m
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lateLocked() {
		return
	}
	a.consumers = append(a.consumers, d)
}

func (a *App) modeOf(m ProvisionMode) ProvisionMode {
	if m != 0 {
		return m
	}
	if a.opts.provision != 0 {
		return a.opts.provision
	}
	return Upsert
}

func (a *App) provision(ctx context.Context) error {
	for _, d := range a.streams {
		if err := a.syncStream(ctx, d); err != nil {
			return err
		}
	}
	for _, d := range a.consumers {
		if err := a.syncConsumer(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) syncStream(ctx context.Context, d streamDecl) error {
	js, name := a.js(), d.cfg.Name
	wrap := func(op string, err error) error { return fmt.Errorf("nats: %s stream %q: %w", op, name, err) }
	switch a.modeOf(d.mode) {
	case Verify:
		s, err := js.Stream(ctx, name)
		if err != nil {
			return wrap("verify", err)
		}
		if diff := drift(d.cfg, s.CachedInfo().Config); len(diff) > 0 {
			return wrap("verify", driftError(diff))
		}
	case CreateOnly:
		_, err := js.CreateStream(ctx, d.cfg)
		if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			s, err := js.Stream(ctx, name)
			if err != nil {
				return wrap("create", err)
			}
			a.warnDrift("stream", name, drift(d.cfg, s.CachedInfo().Config))
			return nil
		}
		if err != nil {
			return wrap("create", err)
		}
	default:
		if _, err := js.CreateOrUpdateStream(ctx, d.cfg); err != nil {
			return wrap("sync", err)
		}
	}
	return nil
}

func (a *App) syncConsumer(ctx context.Context, d consumerDecl) error {
	js := a.js()
	push := d.cfg.DeliverSubject != ""
	wrap := func(op string, err error) error {
		return fmt.Errorf("nats: %s consumer %q on stream %q: %w", op, d.name(), d.stream, err)
	}
	switch a.modeOf(d.mode) {
	case Verify:
		info, err := lookupConsumer(ctx, js, d.stream, d.name())
		if err != nil {
			return wrap("verify", err)
		}
		if diff := drift(consumerWant(d.cfg), info.Config); len(diff) > 0 {
			return wrap("verify", driftError(diff))
		}
	case CreateOnly:
		var err error
		if push {
			_, err = js.CreatePushConsumer(ctx, d.stream, d.cfg)
		} else {
			_, err = js.CreateConsumer(ctx, d.stream, d.cfg)
		}
		if errors.Is(err, jetstream.ErrConsumerExists) {
			info, err := lookupConsumer(ctx, js, d.stream, d.name())
			if err != nil {
				return wrap("create", err)
			}
			a.warnDrift("consumer", d.stream+"/"+d.name(), drift(consumerWant(d.cfg), info.Config))
			return nil
		}
		if err != nil {
			return wrap("create", err)
		}
	default:
		var err error
		if push {
			_, err = js.CreateOrUpdatePushConsumer(ctx, d.stream, d.cfg)
		} else {
			_, err = js.CreateOrUpdateConsumer(ctx, d.stream, d.cfg)
		}
		if err != nil {
			return wrap("sync", err)
		}
	}
	return nil
}

// consumerWant drops the declared fields the server overrides: with BackOff,
// AckWait is always BackOff[0].
func consumerWant(cfg jetstream.ConsumerConfig) jetstream.ConsumerConfig {
	if len(cfg.BackOff) > 0 {
		cfg.AckWait = 0
	}
	return cfg
}

// lookupConsumer looks up a pull or push consumer.
func lookupConsumer(ctx context.Context, js jetstream.JetStream, stream, name string) (*jetstream.ConsumerInfo, error) {
	c, err := js.Consumer(ctx, stream, name)
	if errors.Is(err, jetstream.ErrNotPullConsumer) {
		pc, err := js.PushConsumer(ctx, stream, name)
		if err != nil {
			return nil, err
		}
		return pc.CachedInfo(), nil
	}
	if err != nil {
		return nil, err
	}
	return c.CachedInfo(), nil
}

func (a *App) warnDrift(kind, name string, diff []string) {
	if len(diff) > 0 {
		a.opts.logger.Warn("nats: config drift", kind, name, "fields", strings.Join(diff, ", "))
	}
}

func driftError(diff []string) error {
	return fmt.Errorf("%w: %s", ErrConfigDrift, strings.Join(diff, ", "))
}

// drift lists the fields set in want whose value differs in got, as
// "Field (declared X, server Y)". Zero fields in want are skipped.
func drift(want, got any) []string {
	var out []string
	diffValue("", reflect.ValueOf(want), reflect.ValueOf(got), &out)
	return out
}

var timeType = reflect.TypeFor[time.Time]()

func diffValue(path string, w, g reflect.Value, out *[]string) {
	if w.IsZero() {
		return
	}
	switch {
	case w.Type() == timeType:
		if w.Interface().(time.Time).Equal(g.Interface().(time.Time)) {
			return
		}
	case w.Kind() == reflect.Pointer:
		if w.Elem().IsZero() {
			return // the server drops an empty struct such as &Placement{}
		}
		if !g.IsNil() {
			diffValue(path, w.Elem(), g.Elem(), out)
			return
		}
	case w.Kind() == reflect.Struct:
		for i := range w.NumField() {
			// A json:"-" field, such as StreamSource.Domain, is never sent
			// back as declared.
			if f := w.Type().Field(i); f.IsExported() && f.Tag.Get("json") != "-" {
				diffValue(fieldPath(path, f.Name), w.Field(i), g.Field(i), out)
			}
		}
		return
	case w.Kind() == reflect.Map:
		keys := w.MapKeys()
		slices.SortFunc(keys, func(x, y reflect.Value) int { return strings.Compare(fmt.Sprint(x), fmt.Sprint(y)) })
		for _, k := range keys {
			p := fmt.Sprintf("%s[%v]", path, k)
			if gv := g.MapIndex(k); gv.IsValid() {
				diffValue(p, w.MapIndex(k), gv, out)
			} else {
				*out = append(*out, fmt.Sprintf("%s (declared %+v, server unset)", p, w.MapIndex(k)))
			}
		}
		return
	case w.Kind() == reflect.Slice && w.Len() == g.Len():
		for i := range w.Len() {
			diffValue(fmt.Sprintf("%s[%d]", path, i), w.Index(i), g.Index(i), out)
		}
		return
	case reflect.DeepEqual(w.Interface(), g.Interface()):
		return
	}
	*out = append(*out, fmt.Sprintf("%s (declared %+v, server %+v)", path, show(w), show(g)))
}

func fieldPath(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

// show prints a value, dereferencing a pointer so drift reads as values.
func show(v reflect.Value) any {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "unset"
		}
		v = v.Elem()
	}
	return v.Interface()
}
