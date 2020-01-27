use futures::join;

use riven::RiotApi;
use riven::consts::Region;

pub async fn apex_tiers_summoners(riot_api: RiotApi, region: Region) {

    // // Enter tokio async runtime.
    // let mut rt = tokio::runtime::Runtime::new().unwrap();
    // rt.block_on(async {
    //     // Create RiotApi instance from key string.
    //     let api_key = "RGAPI-01234567-89ab-cdef-0123-456789abcdef";
    //     let riot_api = RiotApi::with_key(api_key);

    //     // Get summoner data.
    //     let summoner = riot_api.summoner_v4()
    //         .get_by_summoner_name(Region::NA, "잘못").await
    //         .expect("Get summoner failed.")
    //         .expect("There is no summoner with that name.");

    //     // Print summoner name.
    //     println!("{} Champion Masteries:", summoner.name);

    //     // Get champion mastery data.
    //     let masteries = riot_api.champion_mastery_v4()
    //         .get_all_champion_masteries(Region::NA, &summoner.id).await
    //         .expect("Get champion masteries failed.");

    //     // Print champioon masteries.
    //     for (i, mastery) in masteries[..10].iter().enumerate() {
    //         println!("{: >2}) {: <9}    {: >7} ({})", i + 1,
    //             mastery.champion_id.to_string(),
    //             mastery.champion_points, mastery.champion_level);
    //     }
    // });
}