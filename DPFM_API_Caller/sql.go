package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-incoterms-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-incoterms-reads-rmq-kube/DPFM_API_Output_Formatter"
	"strings"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var incoterms *[]dpfm_api_output_formatter.Incoterms
	var incotermsText *[]dpfm_api_output_formatter.IncotermsText
	for _, fn := range accepter {
		switch fn {
		case "SingleIncoterms":
			func() {
				incoterms = c.SingleIncoterms(mtx, input, output, errs, log)
			}()
		case "MultipleIncoterms":
			func() {
				incoterms = c.MultipleIncoterms(mtx, input, output, errs, log)
			}()
		case "IncotermsText":
			func() {
				incotermsText = c.IncotermsText(mtx, input, output, errs, log)
			}()
		case "IncotermsTexts":
			func() {
				incotermsText = c.IncotermsTexts(mtx, input, output, errs, log)
			}()
		default:
		}
	}

	data := &dpfm_api_output_formatter.Message{
		Incoterms:     incoterms,
		IncotermsText: incotermsText,
	}

	return data
}

func (c *DPFMAPICaller) SingleIncoterms(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Incoterms {
	where := fmt.Sprintf("WHERE Incoterms = '%s'", input.Incoterms.Incoterms)

	if input.Incoterms.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.Incoterms.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_incoterms_incoterms_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, Incoterms DESC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToIncoterms(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) MultipleIncoterms(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Incoterms {

	if input.Incoterms.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.Incoterms.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_incoterms_incoterms_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, Incoterms DESC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToIncoterms(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) IncotermsText(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.IncotermsText {
	var args []interface{}
	incoterms := input.Incoterms.Incoterms
	incotermsText := input.Incoterms.IncotermsText

	cnt := 0
	for _, v := range incotermsText {
		args = append(args, incoterms, v.Language)
		cnt++
	}

	repeat := strings.Repeat("(?,?),", cnt-1) + "(?,?)"
	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_incoterms_incoterms_text_data
		WHERE (Incoterms, Language) IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToIncotermsText(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) IncotermsTexts(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.IncotermsText {
	var args []interface{}
	incotermsText := input.Incoterms.IncotermsText

	cnt := 0
	for _, v := range incotermsText {
		args = append(args, v.Language)
		cnt++
	}

	repeat := strings.Repeat("(?),", cnt-1) + "(?)"
	rows, err := c.db.Query(
		`SELECT * 
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_incoterms_incoterms_text_data
		WHERE Language IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	//
	data, err := dpfm_api_output_formatter.ConvertToIncotermsText(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
