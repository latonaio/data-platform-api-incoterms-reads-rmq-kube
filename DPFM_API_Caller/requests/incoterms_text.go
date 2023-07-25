package requests

type IncotermsText struct {
	Incoterms     		string  `json:"Incoterms"`
	Language          	string  `json:"Language"`
	IncotermsName		string  `json:"IncotermsName"`
	CreationDate		string	`json:"CreationDate"`
	LastChangeDate		string	`json:"LastChangeDate"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
