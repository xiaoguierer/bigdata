package com.bigdata.es.laosiji.estools.essetting;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;

public class EsSetting {
    /**
     * 更新设置
     * @param client
     * @param index
     * @param settings
     * @return
     */
    public static boolean updateSettingsIndex(Client client, String index, Settings settings){
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        UpdateSettingsResponse response = indicesAdminClient.prepareUpdateSettings(index).setSettings(settings).get();
        return response.isAcknowledged();
    }

}
