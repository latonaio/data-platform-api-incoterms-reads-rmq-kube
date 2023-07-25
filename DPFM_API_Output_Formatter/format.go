package dpfm_api_output_formatter

import (
	"data-platform-api-incoterms-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToIncoterms(rows *sql.Rows) (*[]Incoterms, error) {
	defer rows.Close()
	incoterms := make([]Incoterms, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Incoterms{}

		err := rows.Scan(
			&pm.Incoterms,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &incoterms, nil
		}

		data := pm
		incoterms = append(incoterms, Incoterms{
			Incoterms:				data.Incoterms,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &incoterms, nil
}

func ConvertToIncotermsText(rows *sql.Rows) (*[]IncotermsText, error) {
	defer rows.Close()
	incotermsText := make([]IncotermsText, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.IncotermsText{}

		err := rows.Scan(
			&pm.Incoterms,
			&pm.Language,
			&pm.IncotermsName,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &incotermsText, err
		}

		data := pm
		incotermsText = append(incotermsText, IncotermsText{
			Incoterms:     			data.Incoterms,
			Language:          		data.Language,
			IncotermsName:			data.IncotermsName,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &incotermsText, nil
}
