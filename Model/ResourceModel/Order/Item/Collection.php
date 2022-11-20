<?php

namespace SM\Report\Model\ResourceModel\Order\Item;


use Magento\Eav\Model\Config;
use Magento\Eav\Model\ResourceModel\Entity\Attribute;
use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Framework\Data\Collection\Db\FetchStrategyInterface;
use Magento\Framework\Data\Collection\EntityFactory;
use Magento\Framework\Data\Collection\EntityFactoryInterface;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\Helper;
use Magento\Framework\DB\Select;
use Magento\Framework\Event\ManagerInterface;
use Magento\Framework\Model\ResourceModel\Db\AbstractDb;
use Magento\Framework\Model\ResourceModel\Db\VersionControl\Snapshot;
use Magento\Framework\Stdlib\DateTime\TimezoneInterface;
use Magento\Sales\Model\ResourceModel\Report\OrderFactory;
use Magento\Store\Model\StoreManagerInterface;
use Psr\Log\LoggerInterface;
use SM\Report\Helper\Data;
use Zend_Db_Expr;

class Collection extends \Magento\Sales\Model\ResourceModel\Order\Item\Collection
{

    /**
     * @var \Magento\Sales\Model\ResourceModel\Order\Item\CollectionFactory
     */
    protected $orderItemCollectionFactory;


    /**
     * @var \Magento\Eav\Model\Config
     */
    protected $eavConfig;

    /**
     * @var \SM\Report\Helper\Data
     */
    protected $reportHelper;

    /**
     * @var \Magento\Eav\Model\ResourceModel\Entity\Attribute
     */
    protected $eavAttribute;

    protected $_coreResourceHelper;
    protected $scopeConfig;
    protected $_reportOrderFactory;
    protected $_reportOrderCollection;
    protected $_entityFactory;
    /**
     * Locale date instance
     *
     * @var \Magento\Framework\Stdlib\DateTime\TimezoneInterface
     */
    protected $_localeDate;

    /**
     * Collection constructor.
     *
     * @param \Magento\Framework\Data\Collection\EntityFactoryInterface         $entityFactoryInterface
     * @param \Magento\Sales\Model\ResourceModel\Order\Item\CollectionFactory   $orderItemCollectionFactory
     * @param \Magento\Eav\Model\ResourceModel\Entity\Attribute                 $eavAttribute
     * @param \SM\Report\Helper\Data                                            $reportHelper
     * @param \Magento\Eav\Model\Config                                         $eavConfig
     * @param \Psr\Log\LoggerInterface                                          $logger
     * @param \Magento\Framework\Data\Collection\Db\FetchStrategyInterface      $fetchStrategy
     * @param \Magento\Framework\Event\ManagerInterface                         $eventManager
     * @param \Magento\Framework\Model\ResourceModel\Db\VersionControl\Snapshot $entitySnapshot
     * @param \Magento\Framework\Data\Collection\EntityFactory                  $entityFactory
     * @param \Magento\Framework\DB\Helper                                      $coreResourceHelper
     * @param \Magento\Framework\App\Config\ScopeConfigInterface                $scopeConfig
     * @param \Magento\Store\Model\StoreManagerInterface                        $storeManager
     * @param \Magento\Framework\Stdlib\DateTime\TimezoneInterface              $localeDate
     * @param \Magento\Sales\Model\Order\Config                                 $orderConfig
     * @param \Magento\Reports\Model\ResourceModel\Order\Collection             $reportOrderCollection
     * @param \Magento\Sales\Model\ResourceModel\Report\OrderFactory            $reportOrderFactory
     * @param \Magento\Framework\DB\Adapter\AdapterInterface|null               $connection
     * @param \Magento\Framework\Model\ResourceModel\Db\AbstractDb|null         $resource
     */
    public function __construct(
        EntityFactoryInterface $entityFactoryInterface,
        \Magento\Sales\Model\ResourceModel\Order\Item\CollectionFactory $orderItemCollectionFactory,
        Attribute $eavAttribute,
        Data $reportHelper,
        Config $eavConfig,
        LoggerInterface $logger,
        FetchStrategyInterface $fetchStrategy,
        ManagerInterface $eventManager,
        Snapshot $entitySnapshot,
        EntityFactory $entityFactory,
        Helper $coreResourceHelper,
        ScopeConfigInterface $scopeConfig,
        TimezoneInterface $localeDate,
        \Magento\Reports\Model\ResourceModel\Order\Collection $reportOrderCollection,
        OrderFactory $reportOrderFactory,
        AdapterInterface $connection = null,
        AbstractDb $resource = null
    ) {
        parent::__construct($entityFactoryInterface, $logger, $fetchStrategy, $eventManager, $entitySnapshot, $connection, $resource);
        $this->orderItemCollectionFactory = $orderItemCollectionFactory;
        $this->reportHelper = $reportHelper;
        $this->eavConfig = $eavConfig;
        $this->eavAttribute = $eavAttribute;
        $this->reportHelper = $reportHelper;
        $this->_coreResourceHelper = $coreResourceHelper;
        $this->scopeConfig = $scopeConfig;
        $this->_reportOrderCollection = $reportOrderCollection;
        $this->_entityFactory = $entityFactory;
        $this->_localeDate = $localeDate;
        $this->_reportOrderFactory = $reportOrderFactory;
    }

    /**
     * @param string $range
     * @param mixed  $customStart
     * @param mixed  $customEnd
     * @param int    $isFilter
     *
     * @return $this
     */
    public function prepareSummary($range, $customStart, $customEnd, $isFilter = 0)
    {
        $this->prepareSummaryReport($range, $customStart, $customEnd, $isFilter);

        return $this;
    }

    /**
     * Get range expression
     *
     * @param string $range
     *
     * @return \Zend_Db_Expr
     */
    protected function getRangeExpression($range)
    {
        switch ($range) {
            case '24h':
                $expression = $this->getConnection()->getConcatSql(
                    [
                        $this->getConnection()->getDateFormatSql('{{attribute}}', '%Y-%m-%d %H:'),
                        $this->getConnection()->quote('00'),
                    ]
                );
                break;
            case '7d':
            case '1m':
            case '6w':
                $expression = $this->getConnection()->getDateFormatSql('{{attribute}}', '%Y-%m-%d');
                break;
            case '6m':
            case '1y':
            case '2y':
            case 'custom':
            default:
                $expression = $this->getConnection()->getDateFormatSql('{{attribute}}', '%Y-%m');
                break;
        }

        return $expression;
    }

    /**
     * Retrieve query for attribute with timezone conversion
     *
     * @param string      $range
     * @param string      $attribute
     * @param string|null $from
     * @param string|null $to
     *
     * @return string
     */
    protected function getTZRangeOffsetExpression($range, $attribute, $from = null, $to = null)
    {
        return str_replace(
            '{{attribute}}',
            $this->_reportOrderFactory->create()->getStoreTZOffsetQuery($this->getMainTable(), $attribute, $from, $to),
            $this->getRangeExpression($range)
        );
    }

    /**
     * @param $range
     * @param $customStart
     * @param $customEnd
     * @param $isFilter
     *
     * @return $this
     */
    protected function prepareSummaryReport($range, $customStart, $customEnd, $isFilter)
    {
        $this->setMainTable('sales_order_item');
        $connection = $this->getConnection();
        $this->joinOrderTable();

        /**
         * Reset all columns, because result will group only by 'created_at' field
         */
        $this->getSelect()->reset(Select::COLUMNS);

        $dateRange = $this->getDateRange($range, $customStart, $customEnd);

        $tzRangeOffsetExpression = $this->getTZRangeOffsetExpression(
            $range,
            'main_table.created_at',
            $dateRange['from'],
            $dateRange['to']
        );

        $this->getSelect()
            ->columns(
                [
                    'range'     => $tzRangeOffsetExpression,
                    'item_sold' => $connection->getIfNullSql(
                        'SUM(main_table.qty_ordered-main_table.qty_refunded)',
                        '0'
                    ),
                ]
            )
            ->order('range ASC')
            ->group($tzRangeOffsetExpression);

        $this->addFieldToFilter('sfo.retail_status', [['nin' => [11, 12, 13]], ['null' => true]]);
        $this->addFieldToFilter('sfo.base_total_invoiced', ['neq' => 'NULL']);
        $this->addFieldToFilter('main_table.created_at', $dateRange);

        return $this;
    }

    /**
     * Calculate From and To dates (or times) by given period
     *
     * @param string $range
     * @param string $customStart
     * @param string $customEnd
     * @param bool   $returnObjects
     *
     * @return array
     * @throws \Exception
     * @SuppressWarnings(PHPMD.CyclomaticComplexity)
     */
    public function getDateRange($range, $customStart, $customEnd, $returnObjects = false)
    {
        $array_date_start = explode('/', (string)$customStart);
        $array_date_end = explode('/', (string)$customEnd);


        $date_start_GMT = $this->_localeDate->date($array_date_start[0], null, false);
        $date_end_GMT = $this->_localeDate->date($array_date_end[0], null, false);

        $dateEnd = new \DateTime($array_date_end[1]);
        $dateStart = new \DateTime($array_date_start[1]);


        // go to the end of a day
        $dateEnd->setTime(23, 59, 59);

        $dateStart->setTime(0, 0, 0);

        switch ($range) {
            case '7d':
                // substract 6 days we need to include
                // only today and not hte last one from range
                $dateStart->modify('-6 days');

                $date_start_GMT->modify('-6 days');
                break;
            case '6w':
                for ($i = 1; $i <= 5; $i++) {
                    $dateStart->modify('-7 days');
                    $date_start_GMT->modify('-7 days');
                }
                break;
            case '6m':
                for ($i = 1; $i <= 5; $i++) {
                    $dateStart->modify('-1 month');
                    $date_start_GMT->modify('-1 month');
                }
                break;
            case 'custom':
                $dateStart = $customStart ? $customStart : $dateEnd;
                $dateEnd = $customEnd ? $customEnd : $dateEnd;
                break;

            case '1y':
            case '2y':
                $startMonthDay = explode(
                    ',',
                    (string)$this->scopeConfig->getValue(
                        'reports/dashboard/ytd_start',
                        \Magento\Store\Model\ScopeInterface::SCOPE_STORE
                    )
                );
                $startMonth = isset($startMonthDay[0]) ? (int)$startMonthDay[0] : 1;
                $startDay = isset($startMonthDay[1]) ? (int)$startMonthDay[1] : 1;
                $dateStart->setDate($dateStart->format('Y'), $startMonth, $startDay);
                if ($range == '2y') {
                    $dateStart->modify('-1 year');
                }
                break;
        }
        if ($returnObjects) {
            return [$dateStart, $dateEnd];
        } else {
            return ['from' => $date_start_GMT, 'to' => $date_end_GMT, 'datetime' => true];
        }
    }

    public function getSalesReportFromOrderItemCollection($data, $dateStart, $dateEnd, $itemDetail, $ifFilterTotalValue = false, $onlyFromPos = false)
    {
        // transaction count : là đếm số lần order thực hiện 1 payment ( vd :
        $typeReport = $data['type'];
        $dataFilter = $data['filter'];

        $this->setMainTable('sales_order_item');

        $this->getSelect()->reset(Select::COLUMNS);
        $this->addFieldToFilter('main_table.product_type', ['simple', 'virtual', 'configurable', 'bundle']);
        $this->addFieldToFilter('main_table.parent_item_id', ['null' => true]);
        $this->joinOrderTable();

        if ($onlyFromPos) {
            $this->addFieldToFilter('sfo.retail_id', ['notnull' => true])
                ->addFieldToFilter('sfo.retail_id', ['neq' => '']);
        }

        // doi voi nhung product co kieu configuable hoac bundle :
        // cost duoc tinh theo san pham simple di cung no nen phai tinh rieng cost cho nhung product nay
        $this->getCostForParrentItem();
        if ($typeReport == 'order_item') {
            $this->addFieldToFilter('sfo.shipping_method', 'retailshipping_retailshipping');
        } else {
            $this->addFieldToFilter('sfo.retail_status', [['nin' => [11, 12, 13]], ['null' => true]]);
        }
        // add filter date time
        $this->reportHelper->addDateRangerFilter($this, $dateStart, $dateEnd);
        switch ($typeReport) {
            case "sales_summary":
                $this->joinProductEntityTable();
                $this->addFieldToSelect(['sku', 'name', 'product_type']);
                $this->getSelect()->group('sku');
                break;
            case "register":
                if ($itemDetail == "N/A") {
                    $this->addFieldToFilter('sfo.register_id', ['null' => true]);
                } else {
                    $this->addFieldToFilter('sfo.register_id', $itemDetail);
                }
                $this->getSelect()->joinLeft(
                    ['sregister' => $this->getTable('sm_xretail_register')],
                    'sregister.id = register_id',
                    ['register_name' => 'sregister.name']
                );
                $this->joinProductEntityTable();
                $this->addFieldToSelect(['sku', 'name', 'product_type']);
                $this->getSelect()->group('sku');
                break;
            case "customer":
                if ($itemDetail == "N/A") {
                    $this->addFieldToFilter('sfo.customer_id', ['null' => true]);
                } else {
                    $this->addFieldToFilter('sfo.customer_id', $itemDetail);
                }
                $this->getSelect()->joinLeft(
                    ['cgroup' => $this->getTable('customer_group')],
                    'cgroup.customer_group_id = sfo.customer_group_id',
                    ['customer_group_code' => 'cgroup.customer_group_code']
                );
                $customer_telephone_att_id = $this->eavAttribute->getIdByCode('customer', 'retail_telephone');
                if (!!$customer_telephone_att_id) {
                    $this->getSelect()->joinLeft(
                        ['cusvarchar' => $this->getTable('customer_entity_varchar')],
                        'cusvarchar.entity_id = sfo.customer_id AND `cusvarchar`.`attribute_id` = '.$customer_telephone_att_id,
                        ['customer_telephone' => 'cusvarchar.value']
                    );
                }
                $this->joinProductEntityTable();
                $this->addFieldToSelect(['sku', 'name', 'product_type']);
                $this->getSelect()->group('sku');
                break;
            case "product":
                $this->joinProductEntityTable();
                $this->addFieldToSelect(['sku', 'name', 'product_type']);
                $product_name_id = $this->eavAttribute->getIdByCode('catalog_product', 'name');
                if (!!$product_name_id) {
                    // if magento CE must use product_varchar.entity_id instead of product_varchar.row_id
                    if ($this->reportHelper->isCommunityMagentoEdition()) {
                        $this->getSelect()->joinLeft(
                            ['product_varchar' => $this->getTable('catalog_product_entity_varchar')],
                            'product_varchar.entity_id = main_table.product_id AND product_varchar.store_id = 0 AND `product_varchar`.`attribute_id` = '
                            .$product_name_id,
                            ['product_name' => 'product_varchar.value']
                        );
                    } else {
                        $this->getSelect()->joinLeft(
                            ['product_varchar' => $this->getTable('catalog_product_entity_varchar')],
                            'product_varchar.row_id = main_table.product_id AND product_varchar.store_id = 0 AND `product_varchar`.`attribute_id` = '
                            .$product_name_id,
                            ['product_name' => 'product_varchar.value']
                        );
                    }
                }
                $this->getSelect()->columns(['all_product_name' => "GROUP_CONCAT(main_table.name)"]);
                $this->getSelect()->group('sku');
                break;
            case "manufacturer":
                $this->joinProductEntityTable();
                $this->getSelect()->group('manufacturer_key');
                break;
            case "category":
                $this->getSelect()->joinLeft(
                    ['category_product' => $this->getTable('catalog_category_product')],
                    'category_product.product_id = main_table.product_id',
                    ['category_id' => 'category_product.category_id']
                );
                $category_att_id = $this->eavAttribute->getIdByCode('catalog_category', 'name');
                // if magento CE must use category_varchar.entity_id instead of category_varchar.row_id
                if ($this->reportHelper->isCommunityMagentoEdition()) {
                    $this->getSelect()->joinLeft(
                        ['category_varchar' => $this->getTable('catalog_category_entity_varchar')],
                        'category_product.category_id = category_varchar.entity_id AND category_varchar.store_id = 0  AND category_varchar.attribute_id ='
                        .$category_att_id,
                        ['category_name' => 'category_varchar.value']
                    );
                } else {
                    $this->getSelect()->joinLeft(
                        ['category_varchar' => $this->getTable('catalog_category_entity_varchar')],
                        'category_product.category_id = category_varchar.row_id AND category_varchar.store_id = 0  AND category_varchar.attribute_id ='
                        .$category_att_id,
                        ['category_name' => 'category_varchar.value']
                    );
                }

                $this->getSelect()->group('category_id');
                break;
            case "order_item":
                $this->getSelect()->group('item_id');
                $this->setOrder('item_id', 'ASC');
                $this->getSelect()->joinLeft(
                    ['sm_retail_transaction' => $this->getTable('sm_retail_transaction')],
                    'sm_retail_transaction.order_id = main_table.order_id',
                    ['payment_id'          => 'sm_retail_transaction.payment_id',
                     'payment_title'       => 'sm_retail_transaction.payment_title',
                     'payment_type'        => 'sm_retail_transaction.payment_type',
                     'payment_base_amount' => 'sm_retail_transaction.base_amount',
                    ]
                );
                $this->getSelect()->group('sm_retail_transaction.id');
                $this->setOrder('item_id', 'ASC');
                break;
            default:
                break;
        }
        $this->addDataToSelect($typeReport);
        $this->reportHelper->filterByColumn($this, $dataFilter, $ifFilterTotalValue);

        return $this;
    }

    private function getCostForParrentItem()
    {
        $connection = $this->getResource()->getConnection();

        $totalCostCollection = $this->orderItemCollectionFactory->create();
        $totalCostCollection->addAttributeToSelect('parent_item_id');
        $totalCostCollection->addAttributeToFilter('product_type', 'simple');
        $totalCostCollection->addFieldToFilter('parent_item_id', ['neq' => 'NULL']);
        $totalCostCollection->getSelect()->columns(
            [
                'total_order_cost'     => $connection->getIfNullSql('SUM(base_cost * qty_ordered)', '0'),
                'refunded_items_count' => $connection->getIfNullSql('SUM(qty_refunded)', '0'),
            ]
        );
        $totalCostCollection->getSelect()->group('parent_item_id');

        $select = $this->getSelect();
        $select->joinLeft(
            ['items_order' => $totalCostCollection->getSelect()],
            'items_order.parent_item_id = main_table.item_id',
            [
                'total_cost_for_parent_item' => $connection->getIfNullSql('SUM(items_order.total_order_cost)', '0'),
            ]
        );
    }

    private function addDataToSelect($typeReport)
    {
        $connection = $this->getResource()->getConnection();
        $select = $this->getSelect();
        if ($typeReport == 'order_item') {
            $select->columns(
                [
                    'order_id'                   => 'order_id',
                    'store_id'                   => 'store_id',
                    'order_item_id'              => 'item_id',
                    'product_id'                 => "product_id",
                    'product_name'               => "name",
                    'qty_ordered'                => "qty_ordered",
                    'base_price'                 => 'base_price',
                    'base_row_total'             => 'base_row_total',
                    'base_discount_amount'       => 'base_discount_amount',
                    'base_total_refunded'        => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM( - %s + %s - %s)', 0),
                            $connection->getIfNullSql('main_table.base_amount_refunded', '0'),
                            $connection->getIfNullSql('main_table.base_discount_refunded', '0'),
                            $connection->getIfNullSql('main_table.base_tax_refunded', '0')
                        )
                    ),
                    'base_tax_amount'            => 'base_tax_amount',
                    'created_at'                 => 'created_at',
                    'base_order_grand_total'     => 'sfo.base_grand_total',
                    'base_row_subtotal'          => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM(%s - %s)', 0),
                            $connection->getIfNullSql('main_table.base_row_total', '0'),
                            $connection->getIfNullSql('main_table.base_discount_amount', '0')
                        )
                    ),
                    'base_row_subtotal_incl_tax' => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM(%s + %s- %s - %s + %s - %s)', 0),
                            $connection->getIfNullSql('main_table.base_row_total', '0'),
                            $connection->getIfNullSql('main_table.base_tax_amount', '0'),
                            $connection->getIfNullSql('main_table.base_discount_amount', '0'),
                            $connection->getIfNullSql('main_table.base_amount_refunded', '0'),
                            $connection->getIfNullSql('main_table.base_discount_refunded', '0'),
                            $connection->getIfNullSql('main_table.base_tax_refunded', '0')
                        )
                    ),
                    'base_row_payment_amount'    => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM((%s + %s - %s - %s + %s - %s) / (%s + %s + %s - %s + %s - %s) * %s)', 0),
                            $connection->getIfNullSql('main_table.base_row_total', '0'),
                            $connection->getIfNullSql('main_table.base_tax_amount', '0'),
                            $connection->getIfNullSql('main_table.base_discount_amount', '0'),
                            $connection->getIfNullSql('main_table.base_amount_refunded', '0'),
                            $connection->getIfNullSql('main_table.base_discount_refunded', '0'),
                            $connection->getIfNullSql('main_table.base_tax_refunded', '0'),
                            $connection->getIfNullSql('sfo.base_subtotal', '0'),
                            $connection->getIfNullSql('sfo.base_tax_amount', '0'),
                            $connection->getIfNullSql('sfo.base_shipping_amount', '0'),
                            $connection->getIfNullSql('sfo.base_discount_amount', '0'),
                            $connection->getIfNullSql('sfo.base_discount_per_item', '0'),
                            $connection->getIfNullSql('sfo.base_total_refunded', '0'),
                            $connection->getIfNullSql('sm_retail_transaction.base_amount', '0')
                        )
                    ),
                    'user_id'                    => 'sfo.user_id',
                    'sm_seller_ids'              => 'sfo.sm_seller_ids',
                    'sm_seller_username'         => 'sfo.sm_seller_username',
                    'retail_note'                => 'sfo.retail_note',
                ]
            );
        } else {
            $select->columns(
                [
                    'revenue'                    => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM(%s - %s -(%s - %s))', 0),
                            $connection->getIfNullSql('main_table.base_row_total', '0'),
                            $connection->getIfNullSql('main_table.base_discount_invoiced', '0'),
                            $connection->getIfNullSql('main_table.base_amount_refunded', '0'),
                            $connection->getIfNullSql('main_table.base_discount_refunded', '0')
                        )
                    ),
                    'total_for_discount_percent' => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM(%s - %s -(%s - %s))', 0),
                            $connection->getIfNullSql('main_table.base_row_total', '0'),
                            $connection->getIfNullSql('main_table.base_discount_invoiced', '0'),
                            $connection->getIfNullSql('main_table.base_amount_refunded', '0'),
                            $connection->getIfNullSql('main_table.base_discount_refunded', '0')
                        )
                    ),
                    'base_row_total_product'     => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM(%s + %s- %s)', 0),
                            $connection->getIfNullSql('main_table.base_row_total', '0'),
                            $connection->getIfNullSql('main_table.base_tax_amount', '0'),
                            $connection->getIfNullSql('main_table.base_discount_invoiced', '0')
                        )
                    ),
                    'grand_total'                => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM(%s + %s- %s - (%s + %s - %s))', 0),
                            $connection->getIfNullSql('main_table.base_row_total', '0'),
                            $connection->getIfNullSql('main_table.base_tax_amount', '0'),
                            $connection->getIfNullSql('main_table.base_discount_invoiced', '0'),
                            $connection->getIfNullSql('main_table.base_amount_refunded', 0),
                            $connection->getIfNullSql('main_table.base_tax_refunded', 0),
                            $connection->getIfNullSql('main_table.base_discount_refunded', '0')
                        )
                    ),
                    //'total_tax'       => $connection->getIfNullSql('SUM(main_table.base_tax_amount)', '0'),
                    'total_tax'                  => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM(%s - %s)', 0),
                            $connection->getIfNullSql('main_table.base_tax_amount', '0'),
                            $connection->getIfNullSql('main_table.base_tax_refunded', '0')
                        )
                    ),
                    'total_cost'                 => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM(%s * %s)', 0),
                            $connection->getIfNullSql('main_table.qty_ordered', '0'),
                            $connection->getIfNullSql('main_table.base_cost', '0')
                        )
                    ),
                    //'total_cost' => $connection->getIfNullSql('SUM(main_table.qty_ordered) * SUM(main_table.base_cost)',0),
                    'total_cart_size'            => $connection->getIfNullSql('SUM(main_table.qty_ordered)', '0'),
                    'cart_size'                  => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('%s / %s', 0),
                            $connection->getIfNullSql('SUM(main_table.qty_ordered)', '0'),
                            'COUNT(distinct main_table.order_id)'
                        )
                    ),
                    'customer_count'             => $connection->getIfNullSql('COUNT(distinct sfo.customer_id)', '0'),
                    //'discount_amount' => $connection->getIfNullSql('SUM(main_table.base_discount_amount)', '0'),
                    'discount_amount'            => new Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('%s - %s', 0),
                            $connection->getIfNullSql('SUM(main_table.base_discount_amount)', '0'),
                            $connection->getIfNullSql('SUM(main_table.base_discount_refunded)', '0')
                        )
                    ),
                    'first_sale'                 => "MIN(main_table.created_at)",
                    'item_sold'                  => $connection->getIfNullSql('SUM(main_table.qty_ordered-main_table.qty_refunded)', '0'),
                    'last_sale'                  => "MAX(main_table.created_at)",
                    'order_count'                => 'COUNT(distinct main_table.order_id)',
                    'subtotal_refunded'          => $connection->getIfNullSql('SUM(main_table.base_amount_refunded)', '0'),
                    //'total_refunded'    => $connection->getIfNullSql('SUM(main_table.base_total_refunded)', '0')
                    'total_refund_items'         => $connection->getIfNullSql('SUM(main_table.qty_refunded)', '0'),
                ]
            );
        }

        return $this;
    }

    protected function joinOrderTable()
    {
        $select = $this->getSelect();
        $select->joinLeft(
            ['sfo' => $this->getTable('sales_order')],
            'sfo.entity_id = main_table.order_id',
            ['sfo.retail_status', 'sfo.retail_id', 'sfo.customer_email', 'sfo.customer_id', 'sfo.register_id', 'sfo.base_total_invoiced']
        );

        return $this;
    }

    /**
     * @return $this
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    protected function joinProductEntityTable()
    {
        // get manufacture id
        $manufacturer_att_id = $this->eavConfig->getAttribute('catalog_product', 'manufacturer')
            ->getData('attribute_id');
        $select = $this->getSelect();
        // if magento CE must use product_int.entity_id instead of product_int.row_id
        if ($this->reportHelper->isCommunityMagentoEdition()) {
            $select->joinLeft(
                ['product_int' => $this->getTable('catalog_product_entity_int')],
                'product_int.entity_id = main_table.product_id and product_int.store_id = 0 and product_int.attribute_id = '.$manufacturer_att_id,
                ['manufacturer_key' => 'product_int.value']
            );
        } else {
            $select->joinLeft(
                ['product_int' => $this->getTable('catalog_product_entity_int')],
                'product_int.row_id = main_table.product_id and product_int.store_id = 0 and product_int.attribute_id = '.$manufacturer_att_id,
                ['manufacturer_key' => 'product_int.value']
            );
        }

        $select->joinLeft(
            ['eav_option' => $this->getTable('eav_attribute_option_value')],
            'product_int.value = eav_option.option_id and product_int.store_id = eav_option.store_id',
            ['manufacturer_value' => 'eav_option.value']
        );

        return $this;
    }

    /**
     * XRT-5959: Calculate total distributed tax amount
     *
     * @param string $dateStart
     * @param string $dateEnd
     * @param string $sku
     * @param null   $outletId
     *
     * @return $this
     */
    public function getDistributedTaxPercentAmount($dateStart, $dateEnd, $sku = '', $outletId = null)
    {
        $connection = $this->getConnection();
        $this->getSelect()->reset(Select::COLUMNS);
        $this->getSelect()
            ->columns(
                [
                    'tax_percent'  => 'main_table.tax_percent',
                    'total_amount' => new \Zend_Db_Expr(
                        sprintf(
                            $connection->getIfNullSql('SUM(%s - %s)', 0),
                            $connection->getIfNullSql('main_table.base_tax_amount', '0'),
                            $connection->getIfNullSql('main_table.base_tax_refunded', '0')
                        )
                    ),
                ]
            )
            ->group('main_table.tax_percent')
            ->having('total_amount > 0')
            ->having('tax_percent is not null');

        $this->getSelect()
            ->joinLeft(
                ['so' => $this->getTable('sales_order')],
                'so.entity_id = main_table.order_id',
                [
                    'outlet_id'
                ]
            );

        if (!empty($sku)) {
            $this->addFieldToFilter('sku', $sku);
        }

        if ($outletId !== null && $outletId !== 'null') {
            $this->addFieldToFilter('so.outlet_id', $outletId);
        }

        $this->addFieldToFilter('so.retail_status', [['nin' => [11, 12, 13]], ['null' => true]]);
        $this->addFieldToFilter('so.base_total_invoiced', ['neq' => 'NULL']);

        $dateRange = $this->reportHelper->getReportOrderResource()->getDateRange('custom', $dateStart, $dateEnd);
        $this->addFieldToFilter('main_table.created_at', $dateRange);

        return $this;
    }
}

