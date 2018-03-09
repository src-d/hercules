package hercules

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestCommentSentimentMeta(t *testing.T) {
	sent := CommentSentimentAnalysis{}
	assert.Equal(t, sent.Name(), "Sentiment")
	assert.Equal(t, len(sent.Provides()), 0)
	required := [...]string{DependencyUastChanges, DependencyDay}
	for _, name := range required {
		assert.Contains(t, sent.Requires(), name)
	}
	opts := sent.ListConfigurationOptions()
	matches := 0
	for _, opt := range opts {
		switch opt.Name {
		case ConfigCommentSentimentMinLength, ConfigCommentSentimentGap:
			matches++
		}
	}
	assert.Len(t, opts, matches)
	assert.Equal(t, sent.Flag(), "sentiment")
	assert.Len(t, sent.Features(), 1)
	assert.Equal(t, sent.Features()[0], FeatureUast)
}

func TestCommentSentimentConfigure(t *testing.T) {
	sent := CommentSentimentAnalysis{}
	facts := map[string]interface{}{}
	facts[ConfigCommentSentimentMinLength] = 77
	facts[ConfigCommentSentimentGap] = float32(0.77)
	facts[FactCommitsByDay] = map[int][]plumbing.Hash{}
	sent.Configure(facts)
	assert.Equal(t, sent.Gap, float32(0.77))
	assert.Equal(t, sent.MinCommentLength, 77)
	facts[ConfigCommentSentimentMinLength] = -10
	facts[ConfigCommentSentimentGap] = float32(2)
	sent.Configure(facts)
	assert.Equal(t, sent.Gap, DefaultCommentSentimentGap)
	assert.Equal(t, sent.MinCommentLength, DefaultCommentSentimentCommentMinLength)
}

func TestCommentSentimentRegistration(t *testing.T) {
	tp, exists := Registry.registered[(&CommentSentimentAnalysis{}).Name()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "CommentSentimentAnalysis")
	tp, exists = Registry.flags[(&CommentSentimentAnalysis{}).Flag()]
	assert.True(t, exists)
	assert.Equal(t, tp.Elem().Name(), "CommentSentimentAnalysis")
}