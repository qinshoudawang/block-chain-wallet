package broadcaster

import "time"

// attempt 从 0 开始
func NextBackoff(attempt int) time.Duration {
	switch attempt {
	case 0:
		return 10 * time.Second
	case 1:
		return 30 * time.Second
	case 2:
		return 2 * time.Minute
	case 3:
		return 5 * time.Minute
	default:
		// 上限 30 分钟
		return min(time.Duration(10*(attempt-3))*time.Minute, 30*time.Minute)
	}
}
