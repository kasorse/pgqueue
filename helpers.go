package pgqueue

func getDelayByAttemptIndex(
	progressiveDelays []uint32,
	attemptIndex uint16,
) uint32 {
	length := len(progressiveDelays)

	if int(attemptIndex) < length {
		return progressiveDelays[attemptIndex]
	}

	return progressiveDelays[length-1]
}
