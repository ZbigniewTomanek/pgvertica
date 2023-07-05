package pgvertica

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

type PreparedStatement struct {
	name          string
	query         string
	parameterOIDs []uint32
	parameters    *[]string
	binds         *[]interface{}
}

func (ps *PreparedStatement) getBinds() *[]interface{} {
	if ps.binds == nil {
		binds := make([]interface{}, len(ps.parameterOIDs))
		return &binds
	} else {
		return ps.binds
	}
}

func (ps *PreparedStatement) getQuery() string {
	if len(ps.parameterOIDs) == 0 {
		return ps.query
	} else {
		if ps.parameters == nil || len(*ps.parameters) != len(ps.parameterOIDs) {
			Logger.Warn("inproper prepared statement parameters! will use nulls for format", "ps", ps)
			parametrisedQuery := ps.query
			for i := 0; i < len(ps.parameterOIDs); i++ {
				parametrisedQuery = strings.Replace(parametrisedQuery, fmt.Sprintf("$%d", i+1), "null", 1)
			}
			for i := 0; i < len(ps.parameterOIDs); i++ {
				parametrisedQuery = strings.Replace(parametrisedQuery, "?", "null", 1)
			}
			return parametrisedQuery
		} else {
			parametrisedQuery := ps.query
			for i, arg := range *ps.parameters {
				parametrisedQuery = strings.Replace(parametrisedQuery, "?", arg, 1)
				parametrisedQuery = strings.Replace(parametrisedQuery, fmt.Sprintf("$%d", i+1), "null", 1)
			}
			return parametrisedQuery
		}
	}
}

func parseParameter(format_code int16, parameterOID uint32, param []byte) (string, error) {
	switch format_code {
	case 0: // Text format
		return string(param), nil
	case 1: // Binary format
		switch parameterOID {
		case pgtype.BoolOID:
			if param[0] == 0 {
				return "false", nil
			} else {
				return "true", nil
			}
		case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
			switch len(param) {
			case 2:
				return fmt.Sprint((binary.BigEndian.Uint16(param))), nil
			case 4:
				return fmt.Sprint((binary.BigEndian.Uint32(param))), nil
			case 8:
				return fmt.Sprint(int64(binary.BigEndian.Uint64(param))), nil
			}
		case pgtype.Float4OID, pgtype.Float8OID:
			switch len(param) {
			case 4:
				bits := binary.BigEndian.Uint32(param)
				return fmt.Sprint(float32(math.Float32frombits(bits))), nil
			case 8:
				bits := binary.BigEndian.Uint64(param)
				return fmt.Sprint(math.Float64frombits(bits)), nil
			}
		case pgtype.BPCharOID, pgtype.VarcharOID, pgtype.TextOID:
			return string(param), nil
		default:
			Logger.Warn("data type OID is not explicitly supported", "dataTypeOID", parameterOID, "param", param, "parsed", string(param))
			return string(param), nil
		}
	}

	return "", fmt.Errorf("unsupported format code or data type OID: format_code=%d, dataTypeOID=%d", format_code, parameterOID)
}

func (ps *PreparedStatement) addParameters(msg pgproto3.Bind) error {
	binds := make([]interface{}, len(msg.Parameters))
	params := []string{}
	for i := range msg.Parameters {
		parsed, err := parseParameter(msg.ParameterFormatCodes[i], ps.parameterOIDs[i], msg.Parameters[i])
		if err != nil {
			return err
		}
		binds[i] = parsed
		params = append(params, parsed)
	}
	ps.parameters = &params
	ps.binds = &binds
	return nil
}
