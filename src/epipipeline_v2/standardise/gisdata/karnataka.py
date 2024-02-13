from epipipeline_v2.standardise.gisdata import fuzzy_matching


def get_sd_vill_v1(districtID, subdistrictName, villageName,
                   regionIDs_dict, regionIDs_df, thresholds):

    subdistrict_choices = regionIDs_df[regionIDs_df["parentID"] ==
                                       districtID].reset_index(drop=True)["regionName"].to_list()

    subdistrictName = fuzzy_matching(subdistrictName, subdistrict_choices,
                                     thresholds["subdistrict"])

    if subdistrictName is None:
        return "subdistrict_0", None, "village_0", None

    else:
        subdistrictID = regionIDs_df[(regionIDs_df["regionName"] == subdistrictName)
                                     &
                                     (regionIDs_df["parentID"] == districtID)
                                     ].iloc[0]["regionID"]

        village_choices = regionIDs_df[regionIDs_df["parentID"] ==
                                       subdistrictID].reset_index(drop=True)["regionName"].to_list()

        villageName = fuzzy_matching(villageName, village_choices,
                                     thresholds["village"])

        if villageName is None:

            return subdistrictID, subdistrictName, "village_0", None

        else:

            villageID = regionIDs_df[(regionIDs_df["regionName"] == villageName)
                                     &
                                     (regionIDs_df["parentID"] == subdistrictID)
                                     ].iloc[0]["regionID"]

            return subdistrictID, subdistrictName, villageID, villageName
