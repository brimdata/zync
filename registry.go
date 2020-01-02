package zinger

//XXX TBD
type Registry struct {
	subject string
	servers []string
}

func NewRegistry(subject string, servers []string) *Registry {
	return &Registry{
		subject: subject,
		servers: servers,
	}
}

func (r *Registry) Fetch(id int) (string, error) {
	return "", nil
}
