package drf_test01;

public abstract class AbstractTest {
    public void test(){
        System.out.println("select \"_accountid\" as \"#account_id\", \"_distinctid\" as \"#distinct_id\", \"_eventname\" as \"#event_name\", \"_ip\" as \"#ip\", \"_time\" as \"#time\", 8 as \"#zone_offset\", \"app_plat\" as \"app_plat\", \"ad_channel\" as \"ad_channel\", \"uin\" as \"uin\", \"group_id\" as \"group_id\", \"level\" as \"level\", \"total_pay_money\" as \"total_pay_money\", \"device_os\" as \"device_os\", \"honour\" as \"honour\", \"backflow_channel\" as \"backflow_channel\", \"share_src_ad_channel\" as \"share_src_ad_channel\", \"config_group\" as \"config_group\", 'yxs' as \"app\" from \"yxs\".\"dwd_register_log\" where date ='${day}'");
        System.out.println("second");
        System.out.println("third");
    }

}
