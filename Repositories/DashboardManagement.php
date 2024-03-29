<?php
/**
 * Created by IntelliJ IDEA.
 * User: vjcspy
 * Date: 4/18/17
 * Time: 3:45 PM
 */

namespace SM\Report\Repositories;

use Magento\Framework\App\ObjectManager;
use Magento\Framework\App\RequestInterface;
use Magento\Framework\ObjectManagerInterface;
use Magento\Framework\Stdlib\DateTime\TimezoneInterface;
use Magento\Store\Model\StoreManagerInterface;
use Magento\User\Model\UserFactory;
use SM\Report\Helper\Data;
use SM\Report\Model\ResourceModel\Order\CollectionFactory as ReportOrderCollectionFactory;
use SM\Report\Model\ResourceModel\Order\Item\CollectionFactory as ReportOrderItemCollectionFactory;
use SM\Report\Repositories\Dashboard\Chart;
use SM\XRetail\Helper\DataConfig;
use SM\XRetail\Model\ResourceModel\Outlet\CollectionFactory;
use SM\XRetail\Repositories\Contract\ServiceAbstract;
use SM\Core\Api\Data\SalesReportItem;
use Magento\Catalog\Model\ProductFactory;

/**
 * Class DashboardManagement
 *
 * @package SM\Report\Repositories
 */
class DashboardManagement extends ServiceAbstract
{

    /**
     * @var array
     */
    protected $chartInstanceType = [];

    /**
     * @var \Magento\Framework\ObjectManagerInterface
     */
    private $objectManager;
    /**
     * @var \SM\XRetail\Model\ResourceModel\Outlet\CollectionFactory
     */
    private $outletCollectionFactory;

    /**
     * @var \Magento\Store\Model\ResourceModel\Store\CollectionFactory
     */
    private $storeFactory;

    /**
     * @var \Magento\Store\Model\ResourceModel\Website\CollectionFactory
     */
    private $websiteFactory;

    private $regionCollectionFactory;
    protected $reportHelper;
    protected $userFactory;
    protected $timezoneInterface;
    protected $reportOrderCollectionFactory;
    protected $salesReportOrderItemCollectionFactory;

    /**
     * @var \Magento\Catalog\Model\Product
     */
    protected $productFactory;

    /**
     * DashboardManagement constructor.
     *
     * @param \Magento\Framework\App\RequestInterface                      $requestInterface
     * @param \SM\XRetail\Helper\DataConfig                                $dataConfig
     * @param \Magento\Store\Model\StoreManagerInterface                   $storeManager
     * @param \Magento\Framework\ObjectManagerInterface                    $objectManager
     * @param \SM\XRetail\Model\ResourceModel\Outlet\CollectionFactory     $outletCollectionFactory
     * @param \SM\Report\Model\ResourceModel\Order\CollectionFactory       $reportOrderCollectionFactory
     * @param \SM\Report\Helper\Data                                       $reportHelper
     * @param \SM\Report\Model\ResourceModel\Order\Item\CollectionFactory  $salesReportOrderItemCollectionFactory
     * @param \Magento\Framework\Stdlib\DateTime\TimezoneInterface         $timezoneInterface
     * @param \Magento\User\Model\UserFactory                              $userFactory
     * @param \Magento\Catalog\Model\ProductFactory                        $productFactory
     * @param \Magento\Store\Model\ResourceModel\Store\CollectionFactory   $storeFactory
     * @param \Magento\Store\Model\ResourceModel\Website\CollectionFactory $websiteFactory
     */
    public function __construct(
        RequestInterface $requestInterface,
        DataConfig $dataConfig,
        StoreManagerInterface $storeManager,
        ObjectManagerInterface $objectManager,
        CollectionFactory $outletCollectionFactory,
        ReportOrderCollectionFactory $reportOrderCollectionFactory,
        Data $reportHelper,
        ReportOrderItemCollectionFactory $salesReportOrderItemCollectionFactory,
        TimezoneInterface $timezoneInterface,
        UserFactory $userFactory,
        ProductFactory $productFactory,
        \Magento\Store\Model\ResourceModel\Store\CollectionFactory $storeFactory,
        \Magento\Store\Model\ResourceModel\Website\CollectionFactory $websiteFactory
    ) {
        $this->objectManager                = $objectManager;
        $this->outletCollectionFactory      = $outletCollectionFactory;
        $this->reportHelper                = $reportHelper;
        $this->reportOrderCollectionFactory = $reportOrderCollectionFactory;
        $this->salesReportOrderItemCollectionFactory = $salesReportOrderItemCollectionFactory;
        $this->timezoneInterface            = $timezoneInterface;
        $this->userFactory                 = $userFactory;
        $this->productFactory               = $productFactory;
        $this->storeFactory                 = $storeFactory;
        $this->websiteFactory               = $websiteFactory;
        parent::__construct($requestInterface, $dataConfig, $storeManager);
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function dashboardChartData()
    {
        $data = $this->getRequestData();

        $scope     = $data->getData('scope');
        $period    = $data->getData('period');
        $dateStart = $data->getData('dateStart');
        $dateEnd   = $data->getData('dateEnd');

        if (!$scope || !$period || !$dateStart || !$dateEnd) {
            throw new \Exception("Please define scope,period,date_start,date_end");
        }

        $listScopeChart = [
            'scope'            => $scope,
            'series'           => [],
            'list_date_filter' => [],
            'top_User' => [],
            'product_sold' => [],
            'product_sold_trend_data' => []
        ];

        switch ($scope) {
            case 'store':
                $stores = $this->storeFactory->create()->setOrder('name', 'ASC');
                foreach ($stores as $store) {
                    $chartInstance = $this->createChartInstance();
                    $chartInstance->getDataHelper()
                                   ->setParam('start_date', $dateStart)
                                   ->setParam('end_date', $dateEnd)
                                   ->setParam('store_id', $store->getId());
                    $listScopeChart['list_date_filter'] = $chartInstance->getChartData($period, true);
                    $listScopeChart['series'][]         = [
                        'name'       => $store->getName(),
                        'chart_data' => $chartInstance->getChartData($period)
                    ];
                }
                break;
            case 'website':
                $websites = $this->websiteFactory->create()->setOrder('name', 'ASC');
                foreach ($websites as $website) {
                    $chartInstance = $this->createChartInstance();
                    $chartInstance->getDataHelper()
                                   ->setParam('start_date', $dateStart)
                                   ->setParam('end_date', $dateEnd)
                                   ->setParam('website_id', $website->getId());
                    $listScopeChart['list_date_filter'] = $chartInstance->getChartData($period, true);
                    $listScopeChart['series'][]         = [
                        'name'       => $website->getName(),
                        'chart_data' => $chartInstance->getChartData($period)
                    ];
                }
                break;
            case 'outlet':
                $outletManager = $this->outletCollectionFactory->create()->setOrder('name', 'ASC');
                foreach ($outletManager as $outlet) {
                    $chartInstance = $this->createChartInstance();
                    $chartInstance->getDataHelper()
                                   ->setParam('start_date', $dateStart)
                                   ->setParam('end_date', $dateEnd)
                                   ->setParam('outlet_id', $outlet->getId());
                    $listScopeChart['list_date_filter'] = $chartInstance->getChartData($period, true);
                    $listScopeChart['series'][]         = [
                        'name'       => $outlet->getName(),
                        'chart_data' => $chartInstance->getChartData($period)
                    ];
                }
                break;
            case 'region':
                $regionManager = $this->regionCollectionFactory->create()->setOrder('region_name', 'ASC');
                foreach ($regionManager as $region) {
                    $chartInstance = $this->createChartInstance();
                    $chartInstance->getDataHelper()
                                   ->setParam('start_date', $dateStart)
                                   ->setParam('end_date', $dateEnd)
                                   ->setParam('region_id', $region->getId());
                    $listScopeChart['list_date_filter'] = $chartInstance->getChartData($period, true);
                    $listScopeChart['series'][]         = [
                        'name'       => $region->getData('region_name'),
                        'chart_data' => $chartInstance->getChartData($period)
                    ];
                }
                break;
        }

        $listScopeChart['top_User'] =  $this->getTopUser($dateStart, $dateEnd);
        $listScopeChart['product_sold'] = $this->getProductSoldData($dateStart, $dateEnd);
        $listScopeChart['product_sold_on_pos'] = $this->getProductSoldData($dateStart, $dateEnd, true);

        //get item sold range for product sold
        foreach ($this->getListProductCurrent($listScopeChart['product_sold']) as $sku) {
            $chartInstance = $this->createProductTrendInstance();
            $chartInstance->getDataHelper()
                           ->setParam('start_date', $dateStart)
                           ->setParam('end_date', $dateEnd)
                           ->setParam('product_sold', true)
                           ->setParam('product_sku', $sku);
            $listScopeChart['product_sold_trend_data']['list_date_filter'] = $chartInstance->getChartData($period, true);
            $listScopeChart['product_sold_trend_data']['series'][]         = [
                'name'       => $sku,
                'chart_data' => $chartInstance->getChartData($period, false)
            ];
        }

        $objectManager = ObjectManager::getInstance();
        $store = $this->storeManager->getStore();
        $currency = $objectManager->create('Magento\Directory\Model\CurrencyFactory')
                                  ->create()
                                  ->load($store->getBaseCurrencyCode());
        $listScopeChart['current_currency'] = $currency->getCurrencySymbol();

        return $listScopeChart;
    }

    public function getListProductCurrent($product_sold)
    {
        $list_sku_product_sold = [];
        foreach ($product_sold as $item) {
            $list_sku_product_sold[] = $item['sku'];
        }
        return $list_sku_product_sold;
    }

    protected function getTopUser($dateStart, $dateEnd)
    {
        $option = [];
        $data              = [
            "type"        => "user",
            "item_filter" => null,
            "filter"      => []
        ];
        $array_date_start  = explode('/', (string)$dateStart);
        $array_date_end    = explode('/', (string)$dateEnd);
        $date_start_GMT    = $this->timezoneInterface->date($array_date_start[0], null, false);
        $date_end_GMT      = $this->timezoneInterface->date($array_date_end[0], null, false);
        $topUserCollection = $this->getReportByOrder()
                                  ->getSalesReportFromOrderCollection($data, $date_start_GMT, $date_end_GMT);
        $topUserCollection->getSelect()->order('revenue DESC');
        $topUserCollection->getSelect()->limit(10);

        foreach ($topUserCollection as $user) {
            $option[] = [
                'id'          => $user->getData('user_id'),
                'username'    => $user->getData('user_id'),
                'revenue'     => $user->getData('revenue'),
                'order_count' => $user->getData('order_count'),
                'items_sold'  => $user->getData('item_sold'),
                'cart_value' => $user->getData('revenue') / ($user->getData('order_count') != 0 ? $user->getData('order_count') : 1),
                'cart_size'  => $user->getData('item_sold') / ($user->getData('order_count') != 0 ? $user->getData('order_count') : 1)
            ];
        }
        return $option;
    }

    /**
     * @return \SM\Report\Model\ResourceModel\Order\Collection
     */
    protected function getReportByOrder()
    {
        return $this->reportOrderCollectionFactory->create();
    }

    public function getProductSoldTrendData($dateStart, $dateEnd, $period)
    {
        $period = $this->convertPeriodForProductTrendData($period);

        $dateRanger = $this->reportHelper->getDateRanger(true, $period, $dateStart, $dateEnd, false);

        $data = [];
        $group = [];
        $data['type'] = 'product';
        $data['filter'] = [];

        foreach ($dateRanger as $date) {
            $xGroup = [];
            list($dateStart, $dateEnd, $dateStartGMT, $dateEndGMT) = array_values($date->getData());

            $xGroup['data'] = ['date_start' => $dateStart, 'date_end' => $dateEnd];
            $collection     = $this->getSalesReportByOrderItem()
                                   ->getSalesReportFromOrderItemCollection($data, $dateStartGMT, $dateEndGMT, null);
            if ($collection->count() == 0) {
                $xGroup['value'][] = null;
            } else {
                foreach ($collection as $item) {
                    $xItem = new SalesReportItem();
                    $xItem->addData($item->getData());
                    $this->convertOutputData($data, $item, $xItem);
                    $xGroup['value'][] = $xItem->getData();
                }
            }
            $group[] = $xGroup;
        }
        return $group;
    }

    private function convertPeriodForProductTrendData($period)
    {
        switch ($period) {
            case '7d':
                $period = [
                    'count' => 7,
                    'range_type' => 'day',
                    'type' => 'last'
                ];
                return $period;
            case '6w':
                $period = [
                    'count' => 6,
                    'range_type' => 'week',
                    'type' => 'last'
                ];
                return $period;
            case '6m':
                $period = [
                    'count' => 6,
                    'range_type' => 'month',
                    'type' => 'last'
                ];
                return $period;
            default:
                return $period;
        }
    }

    public function getProductSoldData($dateStart, $dateEnd, $onlyFromPos = false)
    {
        $dateRanger = $this->reportHelper->getDateRanger(false, null, $dateStart, $dateEnd, true);
        $dateStart = $dateRanger['date_start_GMT'];
        $dateEnd   = $dateRanger['date_end_GMT'];

        $data = [];
        $data['type'] = 'product';
        $data['filter'] = [];
        $collection = $this->getSalesReportByOrderItem()
                           ->getSalesReportFromOrderItemCollection($data, $dateStart, $dateEnd, null, false, $onlyFromPos);
        $collection->getSelect()->order('revenue DESC');
        $collection->getSelect()->limit(10);
        $xGroup = [];
        foreach ($collection as $item) {
            $xItem = new SalesReportItem();
            $xItem->addData($item->getData());
            $this->convertOutputData($data, $item, $xItem);
            $xGroup[] = $xItem->getData();
        }
        return $xGroup;
    }

    private function convertOutputData($dataFilter, $item, $xItem = null, $extra_info = null)
    {
        $data       = $item->getData('sku');
        $productModel = $this->productFactory->create()->loadByAttribute('sku', $item->getData('sku'));
        $product_name = '';
        if ($productModel) {
            $product_name = $productModel->getData('name');
        } else {
            $arrayName = explode(",", (string)$item->getData('all_product_name'));
            if (is_array($arrayName) && end($arrayName)) {
                $product_name  = end($arrayName);
            }
        }
        $data_value = [
            'name'         => $product_name,
            'sku'          => $item->getData('sku'),
            'product_type' => $item->getData('product_type'),
            'manufacturer' => $item->getData('manufacturer_value'),
        ];
        if (empty($data) || $data == null) {
            $data = "N/A";
        }
        if (empty($data_value) || $data_value == null) {
            $data_value = "N/A";
        }
        if ($xItem) {
            $xItem->setData("data_report_type", $data);
            $xItem->setData("data_report_value", $data_value);

            return $xItem;
        } else {
            return ["data" => $data, "value" => $data_value];
        }
    }


    /**
     * @return \SM\Report\Repositories\Dashboard\Chart
     */
    protected function createChartInstance()
    {
        /** @var \SM\Report\Repositories\Dashboard\Chart $chartInstance */
        $chartInstance = $this->objectManager->create('SM\Report\Repositories\Dashboard\Chart');
        $chartInstance
            ->setAxisMaps(
                [
                    'x' => 'range',

                    'a' => 'revenue',
                    'b' => 'quantity',
                    'c' => 'customer_count',
                    'd' => 'discount',
                    'e' => 'discount_percent',
                    'f' => 'average_sales',
                    'g' => 'grand_total'
                ]
            )->setDataRows(
                [
                    'revenue',
                    'quantity',
                    'customer_count',
                    'discount',
                    'discount_percent',
                    'average_sales',
                    'grand_total'
                ]
            );

        return $chartInstance;
    }

    /**
     * @return \SM\Report\Repositories\Dashboard\Chart
     */
    protected function createProductTrendInstance()
    {
        /** @var \SM\Report\Repositories\Dashboard\Chart $chartInstance */
        $chartInstance = $this->objectManager->create('SM\Report\Repositories\Dashboard\Chart');
        $chartInstance
            ->setAxisMaps(
                [
                    'x' => 'range',

                    'a' => 'item_sold',
                ]
            )->setDataRows(['item_sold']);

        return $chartInstance;
    }

    protected function getSalesReportByOrderItem()
    {
        return $this->salesReportOrderItemCollectionFactory->create();
    }
}
