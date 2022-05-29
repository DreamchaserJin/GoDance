package search

func KeyWord() []string {
	slice := []string{
		"my", "worthless", "mr", "quit", "take",
	}
	return slice
}
func Title() [][]string {
	slice := [][]string{
		{"my", "dog", "has", "flea"},
		{"maybe", "not", "take"},
		{"my", "dalmation", "is", "so", "him"},
		{"stop", "worthless", "worthless", "garbage"},
		{"my ", "licks", "ate", "stop", "him"},
		{"quit", "buying", "worthless"},
	}
	return slice
}
func Content() [][]string {
	slice := [][]string{
		{"my", "dog", "has", "flea", "quit", "help", "please"},
		{"maybe", "not", "take", "him", "to", "dog", "park", "stupid"},
		{"my", "dalmation", "is", "so", "cute", "I", "love", "him"},
		{"mr", "posting", "stupid", "worthless", "garbage"},
		{"my", "stop", "mr", "ate", "steak", "how", "to", "stop", "him"},
		{"quit", "buying", "worthless", "dog", "food", "stupid"},
	}
	return slice
}

// title与conten倒排得出的文档ID去重集合
func KeyReverseIndex() map[string][]int {
	slice := make(map[string][]int)
	slice["my"] = []int{0, 2, 4}
	slice["worthless"] = []int{3, 5}
	slice["mr"] = []int{3, 4}
	slice["quit"] = []int{0, 5}
	slice["take"] = []int{1}
	return slice
}

func FilterReverseIndex() map[string][]int {
	slice := make(map[string][]int)
	slice["take"] = []int{1}
	return slice
}
