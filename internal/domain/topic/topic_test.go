package topic_test

import (
	"testing"

	"github.com/gophercast/gophercast/internal/domain/topic"
)

func TestNewTopic(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    topic.Topic
		wantErr bool
	}{
		{
			name:    "valid simple name",
			input:   "users",
			want:    topic.Topic{},
			wantErr: false,
		},
		{
			name:    "valid dotted name",
			input:   "user.created",
			want:    topic.Topic{},
			wantErr: false,
		},
		{
			name:    "valid hyphenated",
			input:   "system-status",
			want:    topic.Topic{},
			wantErr: false,
		},
		{
			name:    "empty name",
			input:   "",
			want:    topic.Topic{},
			wantErr: true,
		},
		{
			name:    "name with space",
			input:   "user created",
			want:    topic.Topic{},
			wantErr: true,
		},
		{
			name:    "name with slash",
			input:   "user/created",
			want:    topic.Topic{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := topic.New(tt.input)

			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got.String() != tt.input {
				t.Errorf("New() = %v, want %v", got.String(), tt.input)
			}
		})
	}
}

func TestTopicEquals(t *testing.T) {
	tests := []struct {
		name     string
		topic1   string
		topic2   string
		expected bool
	}{
		{
			name:     "same name topics are equal",
			topic1:   "users",
			topic2:   "users",
			expected: true,
		},
		{
			name:     "different name topics are not equal",
			topic1:   "users",
			topic2:   "orders",
			expected: false,
		},
		{
			name:     "comparison is case-sensitive",
			topic1:   "Users",
			topic2:   "users",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t1, _ := topic.New(tt.topic1)
			t2, _ := topic.New(tt.topic2)

			if t1.Equals(t2) != tt.expected {
				t.Errorf("Equals() = %v, want %v", t1.Equals(t2), tt.expected)
			}
		})
	}
}

func TestNewPattern(t *testing.T) {
	tests := []struct {
		in      string
		wantErr bool
	}{
		{"users", false},
		{"users.created", false},
		{"users.*", false},
		{"users.#", false},
		{"*.created", false},
		{"users.*.v2", false},
		{"", true},
		{"users.", true},
		{"users.#.v2", true}, // # not last
		{"users.bad/seg", true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			_, err := topic.NewPattern(tt.in)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewPattern(%q) err=%v, wantErr=%v", tt.in, err, tt.wantErr)
			}
		})
	}
}

func TestPatternMatches(t *testing.T) {
	tests := []struct {
		pattern string
		topic   string
		want    bool
	}{
		{"users", "users", true},
		{"users", "users.created", false},
		{"users.*", "users.created", true},
		{"users.*", "users.created.v2", false},
		{"users.*", "users", false},
		{"users.#", "users.created", true},
		{"users.#", "users.created.v2", true},
		{"users.#", "users", false},
		{"users.#", "orders.created", false},
		{"*.created", "users.created", true},
		{"*.created", "orders.created", true},
		{"*.created", "users.deleted", false},
	}
	for _, tt := range tests {
		t.Run(tt.pattern+"_vs_"+tt.topic, func(t *testing.T) {
			p, err := topic.NewPattern(tt.pattern)
			if err != nil {
				t.Fatalf("NewPattern: %v", err)
			}
			tp, err := topic.New(tt.topic)
			if err != nil {
				t.Fatalf("New topic: %v", err)
			}
			if got := p.Matches(tp); got != tt.want {
				t.Errorf("Matches=%v, want %v", got, tt.want)
			}
		})
	}
}
