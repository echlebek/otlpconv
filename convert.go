package main

import (
	"io"
	"os"

	commonv1 "github.com/echlebek/opentelemetry-proto-go/otlp/common/v1"
	"github.com/echlebek/opentelemetry-proto-go/otlp/profiles/v1development"
	"github.com/echlebek/opentelemetry-proto-go/otlp/profiles/v1experimental"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}
	var profiles v1experimental.ProfilesData
	if err := protojson.Unmarshal(input, &profiles); err != nil {
		panic(err)
	}
	var outProfiles v1development.ProfilesData
	convert(&profiles, &outProfiles)
	output, err := protojson.Marshal(&outProfiles)
	if err != nil {
		panic(err)
	}
	if _, err := os.Stdout.Write(output); err != nil {
		panic(err)
	}
}

func convert(in *v1experimental.ProfilesData, out *v1development.ProfilesData) {
	dict := new(v1development.ProfilesDictionary)
	for _, xrp := range in.ResourceProfiles {
		out.ResourceProfiles = append(out.ResourceProfiles, convertResourceProfiles(xrp, dict))
	}
}

func convertResourceProfiles(in *v1experimental.ResourceProfiles, dict *v1development.ProfilesDictionary) *v1development.ResourceProfiles {
	out := new(v1development.ResourceProfiles)
	out.Resource = in.Resource
	out.ScopeProfiles = convertScopeProfiles(in.ScopeProfiles, dict)
	out.SchemaUrl = in.SchemaUrl
	return out
}

func convertScopeProfiles(in []*v1experimental.ScopeProfiles, dict *v1development.ProfilesDictionary) []*v1development.ScopeProfiles {
	out := make([]*v1development.ScopeProfiles, 0)
	for _, xsp := range in {
		dsp := new(v1development.ScopeProfiles)
		dsp.Scope = xsp.Scope
		dsp.SchemaUrl = xsp.SchemaUrl
		dsp.Profiles = convertProfiles(xsp.Profiles, dict)
		out = append(out, dsp)
	}
	return out
}

func findInsertProtoMessage[T proto.Message](list *[]T, candidate T) int {
	for i, v := range *list {
		if proto.Equal(v, candidate) {
			return i
		}
	}
	*list = append(*list, candidate)
	return len(*list) - 1
}

func findInsert[T comparable](list *[]T, candidate T) int {
	for i, v := range *list {
		if v == candidate {
			return i
		}
	}
	*list = append(*list, candidate)
	return len(*list) - 1
}

func convertProfiles(in []*v1experimental.ProfileContainer, dict *v1development.ProfilesDictionary) []*v1development.Profile {
	out := make([]*v1development.Profile, 0)
	for _, xpc := range in {
		dp := new(v1development.Profile)
		addToDictionary(xpc, dp, dict)
		dp.ProfileId = xpc.ProfileId
		dp.TimeNanos = int64(xpc.StartTimeUnixNano)
		dp.DurationNanos = int64(xpc.EndTimeUnixNano) - int64(xpc.StartTimeUnixNano)
		dp.DroppedAttributesCount = xpc.DroppedAttributesCount
		dp.OriginalPayloadFormat = xpc.OriginalPayloadFormat
		dp.OriginalPayload = xpc.OriginalPayload
		dp.SampleType = convertSampleType(xpc.Profile.SampleType, xpc.Profile.StringTable, dict)
		dp.PeriodType = convertValueType(xpc.Profile.PeriodType, xpc.Profile.StringTable, dict)
		dp.Period = xpc.Profile.Period
		dp.CommentStrindices = convertComments(xpc.Profile.Comment, xpc.Profile.StringTable, dict)
		dp.DefaultSampleTypeIndex = int32(findInsert(&dict.StringTable, xpc.Profile.StringTable[xpc.Profile.DefaultSampleType]))
		dp.AttributeIndices = convertAttributes(xpc.Attributes, dict)
		out = append(out, dp)
	}
	return out
}

func addToDictionary(pc *v1experimental.ProfileContainer, profile *v1development.Profile, dict *v1development.ProfilesDictionary) {
	addLinkToDictionary(pc, dict)
	addStringToDictionary(pc, dict)
	addAttributeToDictionary(pc, dict)
	addMappingToDictionary(pc, dict)
	addFunctionToDictionary(pc, dict)
	profile.LocationIndices = addLocationToDictionary(pc, dict)
	profile.Sample = addSamplesToDictionary(pc, dict)
}

func addStringToDictionary(pc *v1experimental.ProfileContainer, dict *v1development.ProfilesDictionary) {
	for _, value := range pc.Profile.StringTable {
		findInsert(&dict.StringTable, value)
	}
}

func addFunctionToDictionary(pc *v1experimental.ProfileContainer, dict *v1development.ProfilesDictionary) {
	for _, function := range pc.Profile.Function {
		dfunc := new(v1development.Function)
		name := pc.Profile.StringTable[function.Name]
		dfunc.NameStrindex = int32(findInsert(&dict.StringTable, name))
		systemName := pc.Profile.StringTable[function.SystemName]
		dfunc.SystemNameStrindex = int32(findInsert(&dict.StringTable, systemName))
		filename := pc.Profile.StringTable[function.Filename]
		dfunc.FilenameStrindex = int32(findInsert(&dict.StringTable, filename))
		dfunc.StartLine = function.StartLine
		findInsertProtoMessage(&dict.FunctionTable, dfunc)
	}
}

func addLocationToDictionary(pc *v1experimental.ProfileContainer, dict *v1development.ProfilesDictionary) []int32 {
	result := make([]int32, 0)
	for _, loc := range pc.Profile.Location {
		dloc := new(v1development.Location)
		mapping := pc.Profile.Mapping[loc.MappingIndex]
		idx := lookupMapping(mapping, dict)
		if idx >= 0 {
			dloc.MappingIndex = &idx
		}
		dloc.Address = loc.Address
		for _, line := range loc.Line {
			dline := new(v1development.Line)
			function := pc.Profile.Function[line.FunctionIndex]
			funcIdx := lookupFunction(function, pc.Profile, dict)
			if funcIdx >= 0 {
				dline.FunctionIndex = funcIdx
			}
			dline.Line = line.Line
			dline.Column = line.Column
			dloc.Line = append(dloc.Line, dline)
		}
		dloc.IsFolded = loc.IsFolded
		for _, idx := range loc.Attributes {
			attr := pc.Profile.AttributeTable[idx]
			dloc.AttributeIndices = append(dloc.AttributeIndices, int32(findInsertProtoMessage(&dict.AttributeTable, attr)))
		}
		index := findInsertProtoMessage(&dict.LocationTable, dloc)
		result = append(result, int32(index))
	}
	return result
}

func lookupFunction(function *v1experimental.Function, profile *v1experimental.Profile, dict *v1development.ProfilesDictionary) int32 {
	for i, dfunc := range dict.FunctionTable {
		if functionEqual(function, dfunc, profile, dict) {
			return int32(i)
		}
	}
	return -1
}

func functionEqual(function *v1experimental.Function, dfunc *v1development.Function, profile *v1experimental.Profile, dict *v1development.ProfilesDictionary) bool {
	return (profile.StringTable[function.Name] == dict.StringTable[dfunc.NameStrindex] &&
		profile.StringTable[function.SystemName] == dict.StringTable[dfunc.SystemNameStrindex] &&
		profile.StringTable[function.Filename] == dict.StringTable[dfunc.FilenameStrindex] &&
		function.StartLine == dfunc.StartLine)
}

func lookupMapping(mapping *v1experimental.Mapping, dict *v1development.ProfilesDictionary) int32 {
	for i, dmap := range dict.MappingTable {
		if mapping.MemoryStart == dmap.MemoryStart && mapping.MemoryLimit == dmap.MemoryLimit && mapping.FileOffset == dmap.FileOffset {
			return int32(i)
		}
	}
	return -1
}

func lookupLocation(location *v1experimental.Location, dict *v1development.ProfilesDictionary) int32 {
	for i, loc := range dict.LocationTable {
		if location.Address == loc.Address {
			// this may be too loose
			return int32(i)
		}
	}
	return -1
}

func addSamplesToDictionary(pc *v1experimental.ProfileContainer, dict *v1development.ProfilesDictionary) []*v1development.Sample {
	result := make([]*v1development.Sample, 0)
	for _, sample := range pc.Profile.Sample {
		dsmp := new(v1development.Sample)
		location := pc.Profile.Location[sample.LocationsStartIndex]
		locIdx := lookupLocation(location, dict)
		if locIdx >= 0 {
			dsmp.LocationsStartIndex = locIdx
		}
		dsmp.LocationsLength = int32(sample.LocationsLength)
		dsmp.Value = sample.Value
		if len(pc.Profile.LinkTable) > int(sample.Link) {
			link := pc.Profile.LinkTable[sample.Link]
			dlink := convertLink(link)
			linkIndex := int32(findInsertProtoMessage(&dict.LinkTable, dlink))
			dsmp.LinkIndex = &linkIndex
		}
		dsmp.TimestampsUnixNano = sample.TimestampsUnixNano
		result = append(result, dsmp)
	}
	return result
}

func addAttributeToDictionary(pc *v1experimental.ProfileContainer, dict *v1development.ProfilesDictionary) {
	for _, kv := range pc.Profile.AttributeTable {
		findInsertProtoMessage(&dict.AttributeTable, kv)
	}
}

func addMappingToDictionary(pc *v1experimental.ProfileContainer, dict *v1development.ProfilesDictionary) {
	for _, mapping := range pc.Profile.Mapping {
		dmapping := new(v1development.Mapping)
		dmapping.MemoryStart = mapping.MemoryStart
		dmapping.MemoryLimit = mapping.MemoryLimit
		dmapping.FileOffset = mapping.FileOffset
		filename := pc.Profile.StringTable[mapping.Filename]
		dmapping.FilenameStrindex = int32(findInsert(&dict.StringTable, filename))
		for _, idx := range mapping.Attributes {
			attr := pc.Profile.AttributeTable[idx]
			findInsertProtoMessage(&dict.AttributeTable, attr)
		}
		dmapping.HasFunctions = mapping.HasFunctions
		dmapping.HasFilenames = mapping.HasFilenames
		dmapping.HasLineNumbers = mapping.HasLineNumbers
		dmapping.HasInlineFrames = mapping.HasInlineFrames
		findInsertProtoMessage(&dict.MappingTable, dmapping)
	}
}

func addLinkToDictionary(pc *v1experimental.ProfileContainer, dict *v1development.ProfilesDictionary) {
	for _, link := range pc.Profile.LinkTable {
		dLink := new(v1development.Link)
		dLink.TraceId = link.TraceId
		dLink.SpanId = link.SpanId
		findInsertProtoMessage(&dict.LinkTable, dLink)
	}
}

func convertLink(in *v1experimental.Link) *v1development.Link {
	out := new(v1development.Link)
	out.TraceId = in.TraceId
	out.SpanId = in.SpanId
	return out
}

func convertAttributes(in []*commonv1.KeyValue, dict *v1development.ProfilesDictionary) []int32 {
	out := make([]int32, 0)
	for _, kv := range in {
		out = append(out, int32(findInsertProtoMessage(&dict.AttributeTable, kv)))
	}
	return out
}

func convertComments(in []int64, stringTable []string, dict *v1development.ProfilesDictionary) []int32 {
	out := make([]int32, 0)
	for _, idx := range in {
		out = append(out, int32(findInsert(&dict.StringTable, stringTable[idx])))
	}
	return out
}

func convertValueType(in *v1experimental.ValueType, stringTable []string, dict *v1development.ProfilesDictionary) *v1development.ValueType {
	out := new(v1development.ValueType)
	out.TypeStrindex = int32(findInsert(&dict.StringTable, stringTable[in.Type]))
	out.UnitStrindex = int32(findInsert(&dict.StringTable, stringTable[in.Unit]))
	return out
}

func convertSampleType(in []*v1experimental.ValueType, stringTable []string, dict *v1development.ProfilesDictionary) []*v1development.ValueType {
	out := make([]*v1development.ValueType, 0)
	for _, xs := range in {
		out = append(out, convertValueType(xs, stringTable, dict))
	}
	return out
}
