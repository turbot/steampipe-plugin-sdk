package plugin

func (d *QueryData) getRateLimiterTagValues(hydrateCallTagValues map[string]string) map[string]string {
	//log.Printf("[INFO] hydrateCall %s getRateLimiterTagValues (%s)", h.Name, h.queryData.connectionCallId)
	//
	//
	//log.Printf("[INFO] callSpecificTags: %s (%s)", formatStringMap(callSpecificTags), h.queryData.connectionCallId)
	//log.Printf("[INFO] baseTags: %s (%s)", formatStringMap(baseTags), h.queryData.connectionCallId)

	// make a new map to populate
	allTagValues := make(map[string]string)

	// build list of source value maps which we will merge
	// this is in order of DECREASING precedence, i.e. highest first
	tagValueMaps := []map[string]string{
		// static tag values defined by hydrate config
		hydrateCallTagValues,
		// tag values for this scan (mix of statix and colum tag values)
		d.rateLimiterTagValues,
	}

	for _, tagValues := range tagValueMaps {
		for k, v := range tagValues {
			// only set tag if not already set - earlier tag values have precedence
			if _, gotTag := allTagValues[k]; !gotTag {
				allTagValues[k] = v
			}
		}
	}
	return allTagValues
}
