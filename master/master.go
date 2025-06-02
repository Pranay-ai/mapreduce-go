package master

type MasterNode struct {
	input_file_path  string
	output_file_path string
	num_of_reducers  int
	workers          []Worker
	plugin_file_path string
}

func NewMasterNode(input_file_path string, output_file_path string, num_of_reducers int, plugin_file_path string) *MasterNode {
	return &MasterNode{
		input_file_path:  input_file_path,
		output_file_path: output_file_path,
		num_of_reducers:  num_of_reducers,
		workers:          make([]Worker, 0),
		plugin_file_path: plugin_file_path,
	}
}
func (m *MasterNode) AddWorker(worker Worker) {
	m.workers = append(m.workers, worker)
}
