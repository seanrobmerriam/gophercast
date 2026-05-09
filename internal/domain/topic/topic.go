package topic

import (
	"errors"
	"regexp"
	"strings"
)

// Topic represents a named channel of communication in the pub/sub system.
// Topics are identified by strings and must follow naming rules.
type Topic struct {
	name string
}

// New creates a new Topic with the given name.
// The name must not be empty and must contain only letters, numbers, dots, and hyphens.
func New(name string) (Topic, error) {
	if name == "" {
		return Topic{}, errors.New("topic name cannot be empty")
	}

	if !isValidTopicName(name) {
		return Topic{}, errors.New("invalid topic name: must contain only letters, numbers, dots, and hyphens")
	}

	return Topic{name: name}, nil
}

// String returns the topic name.
func (t Topic) String() string {
	return t.name
}

// Equals returns true if two topics have the same name.
func (t Topic) Equals(other Topic) bool {
	return t.name == other.name
}

// AsPattern returns a Pattern that matches only this exact topic.
func (t Topic) AsPattern() Pattern {
	return Pattern{raw: t.name, segments: strings.Split(t.name, ".")}
}

// FromString builds a Topic from a raw string without validation.
// Intended for internal pub/sub plumbing (e.g. labelling a subscription
// created from a wildcard Pattern). Application code should use New.
func FromString(name string) Topic {
	return Topic{name: name}
}

// isValidTopicName checks if the topic name follows the naming rules.
func isValidTopicName(name string) bool {
	// Only allow letters, numbers, dots, and hyphens
	pattern := `^[a-zA-Z0-9.-]+$`
	matched, _ := regexp.MatchString(pattern, name)
	return matched
}

// Pattern is a topic matcher that supports wildcard segments.
//
// Two wildcards are supported, separated by '.':
//
//   - "*" matches exactly one segment (e.g. "users.*" matches "users.created"
//     but not "users.created.v2" or "users").
//   - "#" matches one or more trailing segments and may only appear as the
//     final segment (e.g. "users.#" matches "users.created" and
//     "users.created.v2").
//
// A pattern with no wildcards behaves as an exact match.
type Pattern struct {
	raw      string
	segments []string
}

var patternSegment = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

// NewPattern parses a subscription pattern.
func NewPattern(s string) (Pattern, error) {
	if s == "" {
		return Pattern{}, errors.New("pattern cannot be empty")
	}
	segs := strings.Split(s, ".")
	for i, seg := range segs {
		switch seg {
		case "":
			return Pattern{}, errors.New("pattern segment cannot be empty")
		case "*":
			// single-segment wildcard, allowed anywhere
		case "#":
			if i != len(segs)-1 {
				return Pattern{}, errors.New("'#' wildcard is only allowed as the final segment")
			}
		default:
			if !patternSegment.MatchString(seg) {
				return Pattern{}, errors.New("pattern segments must contain only letters, numbers, and hyphens")
			}
		}
	}
	return Pattern{raw: s, segments: segs}, nil
}

// String returns the original pattern string.
func (p Pattern) String() string {
	return p.raw
}

// Matches reports whether the pattern matches the given topic.
func (p Pattern) Matches(t Topic) bool {
	topicSegs := strings.Split(t.name, ".")
	patSegs := p.segments
	i := 0
	for i < len(patSegs) {
		seg := patSegs[i]
		if seg == "#" {
			return i < len(topicSegs)
		}
		if i >= len(topicSegs) {
			return false
		}
		if seg != "*" && seg != topicSegs[i] {
			return false
		}
		i++
	}
	return i == len(topicSegs)
}
