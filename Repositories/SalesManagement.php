<?php

namespace SM\Report\Repositories;

use Exception;
use Magento\Catalog\Api\CategoryRepositoryInterface;
use Magento\Catalog\Model\CategoryFactory;
use Magento\Catalog\Model\ProductFactory;
use Magento\Customer\Api\AddressRepositoryInterface;
use Magento\Customer\Api\CustomerRepositoryInterface;
use Magento\Framework\App\ObjectManager;
use Magento\Framework\App\RequestInterface;
use Magento\Framework\Locale\ListsInterface;
use Magento\Payment\Helper\Data;
use Magento\Store\Model\StoreManagerInterface;
use Magento\User\Model\UserFactory;
use SM\Core\Api\Data\SalesReportItem;
use SM\Payment\Model\RetailPayment;
use SM\Payment\Model\RetailPaymentRepository;
use SM\Report\Model\ResourceModel\Order\CollectionFactory;
use SM\Report\Model\ResourceModel\Order\Item\CollectionFactory as SaleReportOrderItemCollectionFactory;
use SM\Shift\Model\ResourceModel\RetailTransaction\CollectionFactory as TransactionCollectionFactory;
use SM\XRetail\Helper\DataConfig;
use SM\XRetail\Repositories\Contract\ServiceAbstract;

class SalesManagement extends ServiceAbstract
{
    const GROUP_BY_ORDER = "sales_summary,user,outlet,register,customer,customer_group,magento_website,magento_storeview,payment_method,shipping_method,order_status,currency,day_of_week,hour,region,reference_number,motetary";

    const GROUP_BY_ITEM = "product,manufacturer,category,order_item";
    /**
     * @var SM\Report\Model\ResourceModel\Order\CollectionFactory
     */
    protected $salesReportOrderCollectionFactory;

    /**
     * @var \SM\Report\Model\ResourceModel\Order\Item\CollectionFactory
     */
    protected $salesReportOrderItemCollectionFactory;
    /**
     * @var SM\Report\Helper\Data
     */
    protected $reportHelper;
    /**
     * @var \SM\Core\Api\Data\SalesReportItemFactory
     */
    protected $salesReportItemFactory;
    /**
     * @var \Magento\Customer\Api\CustomerRepositoryInterface
     */
    private $customerRepositoryInterface;

    /**
     * @var \Magento\Catalog\Api\CategoryRepositoryInterface
     */
    private $categoryRepositoryInterface;

    /*
    * @var \Magento\Payment\Helper\Data
    */
    protected $paymentHelper;

    /**
     * @var \SM\Payment\Model\RetailPaymentRepository
     */
    protected $retailPaymentRepository;

    /**
     * @var \SM\Shift\Model\ResourceModel\RetailTransaction\CollectionFactory
     */
    protected $transactionCollectionFactory;

    /**
     * @var \Magento\Framework\Locale\ListsInterface
     */
    protected $localeLists;

    /**
     * @var \Magento\User\Model\UserFactory
     */
    protected $userFactory;
    /**
     * @var \Magento\Catalog\Model\Product
     */
    protected $productFactory;
    /**
     * @var \Magento\Catalog\Model\Category
     */
    protected $categoryFactory;

    /**
     * @var \Magento\Customer\Api\AddressRepositoryInterface
     */
    protected $addressRepository;
    
    /**
     * SalesManagement constructor.
     *
     * @param \Magento\Framework\App\RequestInterface $requestInterface
     * @param \SM\XRetail\Helper\DataConfig $dataConfig
     * @param \Magento\Store\Model\StoreManagerInterface $storeManager
     * @param \Magento\Framework\Locale\ListsInterface $localeLists
     * @param \SM\Report\Model\ResourceModel\Order\CollectionFactory $salesReportOrderCollectionFactory
     * @param \SM\Report\Model\ResourceModel\Order\Item\CollectionFactory $salesReportOrderItemCollectionFactory
     * @param \Magento\Customer\Api\CustomerRepositoryInterface $customerRepositoryInterface
     * @param \Magento\Catalog\Api\CategoryRepositoryInterface $categoryRepositoryInterface
     * @param \Magento\Payment\Helper\Data $paymentHelper
     * @param \SM\Report\Helper\Data $reportHelper
     * @param \SM\Payment\Model\RetailPaymentRepository $retailPaymentRepository
     * @param \Magento\User\Model\UserFactory $userFactory
     * @param \Magento\Catalog\Model\ProductFactory $productFactory
     * @param \Magento\Catalog\Model\CategoryFactory $categoryFactory
     * @param \SM\Shift\Model\ResourceModel\RetailTransaction\CollectionFactory $transactionCollectionFactory
     * @param \Magento\Customer\Api\AddressRepositoryInterface $addressRepository
     * @param \SM\Core\Api\Data\SalesReportItemFactory $salesReportItemFactory
     */
    public function __construct(
        RequestInterface $requestInterface,
        DataConfig $dataConfig,
        StoreManagerInterface $storeManager,
        ListsInterface $localeLists,
        CollectionFactory $salesReportOrderCollectionFactory,
        SaleReportOrderItemCollectionFactory $salesReportOrderItemCollectionFactory,
        CustomerRepositoryInterface $customerRepositoryInterface,
        CategoryRepositoryInterface $categoryRepositoryInterface,
        Data $paymentHelper,
        \SM\Report\Helper\Data $reportHelper,
        RetailPaymentRepository $retailPaymentRepository,
        UserFactory $userFactory,
        ProductFactory $productFactory,
        CategoryFactory $categoryFactory,
        TransactionCollectionFactory $transactionCollectionFactory,
        AddressRepositoryInterface $addressRepository,
        \SM\Core\Api\Data\SalesReportItemFactory $salesReportItemFactory
    ) {
        $this->transactionCollectionFactory = $transactionCollectionFactory;
        $this->retailPaymentRepository = $retailPaymentRepository;
        $this->salesReportOrderItemCollectionFactory = $salesReportOrderItemCollectionFactory;
        $this->salesReportOrderCollectionFactory = $salesReportOrderCollectionFactory;
        $this->customerRepositoryInterface = $customerRepositoryInterface;
        $this->categoryRepositoryInterface = $categoryRepositoryInterface;
        $this->reportHelper = $reportHelper;
        $this->paymentHelper = $paymentHelper;
        $this->userFactory = $userFactory;
        $this->productFactory = $productFactory;
        $this->categoryFactory = $categoryFactory;
        $this->localeLists = $localeLists;
        $this->addressRepository = $addressRepository;
        parent::__construct($requestInterface, $dataConfig, $storeManager);
        $this->salesReportItemFactory = $salesReportItemFactory;
    }

    /**
     * @return array
     * @throws \Magento\Framework\Exception\LocalizedException
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     * @throws \ReflectionException
     */
    public function salesReportData()
    {
        $data = $this->getRequestData();
        $reportType = $data->getData('type');
        $is_date_compare = $data->getData('is_date_compare');
        $date_start = $data->getData('date_start');
        $date_end = $data->getData('date_end');
        $period_data = $data->getData('period_data');
        $item_detail = $data->getData('item_filter');
        $extra_info = $data->getData('extra_info');

        if (!$reportType || !$date_start || !$date_end || !$period_data) {
            throw  new Exception("Please define the require data");
        }
        $listGroupByItem = explode(",", self::GROUP_BY_ITEM);

        $dateRanger = $this->reportHelper->getDateRanger($is_date_compare, $period_data, $date_start, $date_end, false);
        $dateRangerForGroupData = $this->reportHelper->getDateRanger(
            $is_date_compare,
            $period_data,
            $date_start,
            $date_end,
            true
        );
        if (in_array($reportType, $listGroupByItem)) {
            if ($item_detail == null) {
                $dataGroupBy = $this->getSalesReportFromItems($data, $dateRangerForGroupData, true);
                $items = $this->getSalesReportFromItems($data, $dateRanger, false, null, $dataGroupBy);
            } else {
                $dataGroupBy = $this->getSalesReportFromOrder($data, $dateRangerForGroupData, true, $item_detail, null, $extra_info);
                $items = $this->getSalesReportFromOrder($data, $dateRanger, false, $item_detail, $dataGroupBy, $extra_info);
            }
        } else {
            if ($item_detail == null) {
                $dataGroupBy = $this->getSalesReportFromOrder($data, $dateRangerForGroupData, true);
                $items = $this->getSalesReportFromOrder($data, $dateRanger, false, null, $dataGroupBy);
            } else {
                // in case when select show product button
                if (in_array($reportType, ['register', 'customer', 'sales_summary'])) {
                    $dataGroupBy = $this->getSalesReportFromItems($data, $dateRangerForGroupData, true, $item_detail);
                    $items = $this->getSalesReportFromItems($data, $dateRanger, false, $item_detail, $dataGroupBy);
                } else {
                    if (in_array($reportType, ['region'])) {
                        $dataGroupBy = $this->getSalesReportFromOrder($data, $dateRangerForGroupData, true, $item_detail);
                        $items = $this->getSalesReportFromOrder($data, $dateRanger, false, $item_detail, $dataGroupBy);
                    } else {
                        $dataGroupBy = $this->getSalesReportFromOrder($data, $dateRangerForGroupData, true, $item_detail);
                        $items = $this->getSalesReportFromOrder($data, $dateRanger, false, $item_detail);
                    }
                }
            }
        }

        $objectManager = ObjectManager::getInstance();
        $storeForGetCurrency = $this->storeManager->getStore();
        $currency = $objectManager->create('Magento\Directory\Model\CurrencyFactory')
            ->create()
            ->load($storeForGetCurrency->getBaseCurrencyCode());
        $currencySymbol = $currency->getCurrencySymbol();

        return $this->getSearchResult()
            ->setItems($items)
            ->setGroupDataReport($dataGroupBy)
            ->setBaseCurrency($currencySymbol)
            ->setDateRangerReport(
                [
                    "date_start" => $dateRangerForGroupData['date_start'],
                    "date_end"   => $dateRangerForGroupData['date_end'],
                ]
            )
            ->getOutputReport($item_detail);
    }

    /**
     * @param      $data
     * @param      $dateRanger
     *
     * @param bool $is_getGroupData
     * @param null $itemDetail
     * @param null $dataGroupBy
     * @param null $extra_info
     *
     * @return array
     * @throws \Magento\Framework\Exception\LocalizedException
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     */
    public function getSalesReportFromOrder($data, $dateRanger, $is_getGroupData = false, $itemDetail = null, $dataGroupBy = null, $extra_info = null)
    {
        $reportType = $data['type'];
        $group = [];
        if ($is_getGroupData) {
            $dateStart = $dateRanger['date_start_GMT'];
            $dateEnd = $dateRanger['date_end_GMT'];
            if ($itemDetail == "retailmultiple" && $reportType == "payment_method") {
                $collection = $this->getRetailPaymentMethodCollection($dateStart, $dateEnd, $data['outlet_id']);
            } else {
                $isSelectDetail = false;
                if ($itemDetail != null) {
                    $isSelectDetail = true;
                }
                if ($itemDetail == null) {
                    $collection = $this->getSalesReportByOrder()
                        ->getSalesReportFromOrderCollection($data, $dateStart, $dateEnd, $isSelectDetail, true);
                } else {
                    $collection = $this->getSalesReportByOrder()
                        ->getSalesReportFromOrderCollection($data, $dateStart, $dateEnd, $isSelectDetail, false, $itemDetail, $extra_info);
                }
            }
            foreach ($collection as $item) {
                $key = false;
                if ($reportType == 'monetary') {
                    $key = array_search($item->getData('order_id'), array_column($group, 'data'));
                }
                if ('integer' != getType($key)) {
                    $group[] = $this->convertOutputData($data, $item, null, $extra_info);
                }
            }
        } else {
            foreach ($dateRanger as $date) {
                $xGroup = [];
                list($dateStart, $dateEnd, $dateStartGMT, $dateEndGMT) = array_values($date->getData());
                // de ntn de luc hien thi tren report lay dung duoc current time ( sau khi da convert timezone theo magento 2 lan )
                $xGroup['data'] = ['date_start' => $dateStart, 'date_end' => $dateEnd];
                if ($itemDetail == "retailmultiple" && $reportType == "payment_method") {
                    $collection = $this->getRetailPaymentMethodCollection($dateStartGMT, $dateEndGMT, $data['outlet_id']);
                } else {
                    $isSelectDetail = false;
                    if ($itemDetail == "magento_status" && $reportType == "order_status") {
                        $isSelectDetail = true;
                    }
                    if ($itemDetail == null) {
                        $collection = $this->getSalesReportByOrder()
                            ->getSalesReportFromOrderCollection($data, $dateStartGMT, $dateEndGMT, $isSelectDetail, false);
                    } else {
                        $isSelectDetail = true;
                        $collection = $this->getSalesReportByOrder()
                            ->getSalesReportFromOrderCollection($data, $dateStartGMT, $dateEndGMT, $isSelectDetail, false, $itemDetail, $extra_info);
                    }
                    if (!$isSelectDetail) {
                        $this->addDataFilterItems($data, $dataGroupBy, $collection);
                    }
                }
                if ($collection->count() == 0) {
                    $xGroup['value'][] = null;
                } else {
                    if ($reportType == "payment_method") {
                        foreach ($collection as $item) {
                            if ($data['item_filter']) {
                                $paymentMethod = $this->retailPaymentRepository->getById($item->getData('payment_id'));
                                $dataReportType = $item->getData('payment_id');
                                $dataReportValue = $paymentMethod->getData('title');
                            } else {
                                $dataReportType = $item->getData('payment_method');
                                $listPayment = $this->paymentHelper->getPaymentMethodList();
                                $dataReportValue = isset($listPayment[$dataReportType]) ? $this->paymentHelper->getMethodInstance($dataReportType)->getTitle() . " ($dataReportType)" : $dataReportType;
                            }
                            $xItem = [
                                'data_report_type'  => $dataReportType,
                                'data_report_value' => $dataReportValue,
                                'grand_total'       => $item->getData('grand_total'),
                                'order_count'       => $item->getData('order_count')];
                            $xGroup['value'][] = $xItem;
                        }
                    } else {
                        if ($reportType == "monetary") {
                            $xGroup['value'] = [];
                            foreach ($collection as $item) {
                                $key = array_search($item->getData('order_id'), array_column($xGroup['value'], 'order_id'));
                                if ($key !== false) {
                                    array_push(
                                        $xGroup['value'][$key]['payments'],
                                        [
                                            'payment_id'          => $item->getData('payment_id'),
                                            'payment_title'       => $item->getData('payment_title'),
                                            'payment_type'        => $item->getData('payment_type'),
                                            'payment_base_amount' => $item->getData('payment_base_amount'),
                                        ]
                                    );
                                } else {
                                    $payments = [];
                                    if ($item->getData('payment_id')) {
                                        array_push(
                                            $payments,
                                            [
                                                'payment_id'          => $item->getData('payment_id'),
                                                'payment_title'       => $item->getData('payment_title'),
                                                'payment_type'        => $item->getData('payment_type'),
                                                'payment_base_amount' => $item->getData('payment_base_amount'),
                                            ]
                                        );
                                    }
                                    $item->setData('payments', $payments);
                                    $customer = $this->customerRepositoryInterface->getById($item->getData('customer_id'));
                                    if ($customer) {
                                        $billingAddressId = $customer->getDefaultBilling();
                                        $item->setData('customer_email', $customer->getEmail());
                                        $item->setData('customer_name', $customer->getFirstname() . " " . $customer->getLastName());
                                        $item->setData('customer_phone', $billingAddressId ? $this->addressRepository->getById($billingAddressId)->getTelephone() : '');
                                        $item->setData('customer_group_code', $item->getData('customer_group_code'));
                                        $item->setData('data_report_type', $item->getData('order_id'));
                                    }
                                    $item->setData('store_view_code', $item->getStore()->getCode());
                                    $xGroup['value'][] = $item->getData();
                                }
                            }
                        } else {
                            foreach ($collection as $item) {
                                $xItem = $this->salesReportItemFactory->create();
                                $xItem->addData($item->getData());
                                $this->convertOutputData($data, $item, $xItem, $extra_info);
                                $grossProfit = $item->getData('revenue') - $item->getData('total_cost');
                                $xItem->setData('gross_profit', floatval($grossProfit));
                                if ($item->getData('revenue') != 0) {
                                    $xItem->setData('margin', $grossProfit / ($item->getData('revenue')));
                                } else {
                                    $xItem->setData('margin', $grossProfit / (1));
                                }
                                if ($item->getData('order_count') != 0) {
                                    $xItem->setData(
                                        'cart_value',
                                        $item->getData('revenue') / ($item->getData('order_count'))
                                    );
                                } else {
                                    $xItem->setData(
                                        'cart_value',
                                        $item->getData('revenue') / (1)
                                    );
                                }
                                if ($item->getData('order_count') != 0) {
                                    $xItem->setData(
                                        'cart_value_incl_tax',
                                        $item->getData('grand_total') / ($item->getData('order_count'))
                                    );
                                } else {
                                    $xItem->setData(
                                        'cart_value_incl_tax',
                                        $item->getData('grand_total') / (1)
                                    );
                                }
                                $xItem->setData('discount_amount', -$item->getData('discount_amount'));
                                if ((-$item->getData('discount_amount') + $item->getData('grand_total')) != 0) {
                                    $discount_percent = -$item->getData('discount_amount') / ((-$item->getData('discount_amount') + $item->getData('grand_total')));
                                } else {
                                    $discount_percent = -$item->getData('discount_amount') / (1);
                                }
                                if ($reportType == 'outlet' && !empty($itemDetail)) {
                                    if ((-$item->getData('discount_amount') + $item->getData('base_row_total_product')) != 0) {
                                        $discount_percent = -$item->getData('discount_amount') / ((-$item->getData('discount_amount') + $item->getData('base_row_total_product')));
                                    } else {
                                        $discount_percent = -$item->getData('discount_amount') / (1);
                                    }
                                }
                                $xItem->setData('discount_percent', $discount_percent);
                                if ($item->getData('total_cart_size') != 0) {
                                    $xItem->setData(
                                        'return_percent',
                                        $item->getData('total_refund_items') / ($item->getData('total_cart_size'))
                                    );
                                } else {
                                    $xItem->setData(
                                        'return_percent',
                                        $item->getData('total_refund_items') / (1)
                                    );
                                }
                                $xItem->setData('return_count', $item->getData('total_refund_items'));

                                // XRT-5959: Tax and payment distribution amounts
                                $xItem->setData('distributed_tax_percent_amounts', $this->getDistributedTaxPercentAmounts($dateStartGMT, $dateEndGMT, $data['outlet_id']));
                                $xItem->setData('distributed_payment_amounts', $this->getDistributedPaymentAmounts($dateStartGMT, $dateEndGMT, $xItem->getData('grand_total'), $data['outlet_id']));

                                $xGroup['value'][] = $xItem->getData();
                            }
                        }
                    }
                }
                $group[] = $xGroup;
            }
        }

        return $group;
    }

    /**
     * @return \SM\Report\Model\ResourceModel\Order\Collection
     */
    protected function getSalesReportByOrder()
    {
        return $this->salesReportOrderCollectionFactory->create();
    }

    public function getSalesReportFromItems($data, $dateRanger, $is_getGroupData = false, $itemDetail = null, $dataGroupBy = null)
    {
        $reportType = $data['type'];
        $group = [];
        if ($is_getGroupData) {
            $dateStart = $dateRanger['date_start_GMT'];
            $dateEnd = $dateRanger['date_end_GMT'];

            if ($itemDetail == null) {
                $collection = $this->getSalesReportByOrderItem()
                    ->getSalesReportFromOrderItemCollection(
                        $data,
                        $dateStart,
                        $dateEnd,
                        $itemDetail,
                        true
                    );
            } else {
                $collection = $this->getSalesReportByOrderItem()
                    ->getSalesReportFromOrderItemCollection($data, $dateStart, $dateEnd, $itemDetail);
            }
            foreach ($collection as $item) {
                $key = false;
                if ($reportType == 'order_item') {
                    $key = array_search($item->getData('order_item_id'), array_column($group, 'data'));
                }
                if ('integer' != getType($key)) {
                    $group[] = $this->convertOutputData($data, $item);
                }
            }
        } else {
            foreach ($dateRanger as $date) {
                $xGroup = [];
                list($dateStart, $dateEnd, $dateStartGMT, $dateEndGMT) = array_values($date->getData());

                $xGroup['data'] = ['date_start' => $dateStart, 'date_end' => $dateEnd];
                $collection = $this->getSalesReportByOrderItem()
                    ->getSalesReportFromOrderItemCollection($data, $dateStartGMT, $dateEndGMT, $itemDetail);
                if (!$itemDetail) {
                    $this->addDataFilterItems($data, $dataGroupBy, $collection);
                }
                if ($collection->count() == 0) {
                    $xGroup['value'][] = null;
                } else {
                    $orderItems = $collection->getItems();

                    if ($reportType == 'order_item') {
                        $xGroup['value'] = [];
                        foreach ($collection as $item) {
                            $key = array_search($item->getData('order_item_id'), array_column($xGroup['value'], 'order_item_id'));
                            if ($key !== false) {
                                array_push(
                                    $xGroup['value'][$key]['payments'],
                                    [
                                        'payment_id'          => $item->getData('payment_id'),
                                        'payment_title'       => $item->getData('payment_title'),
                                        'payment_type'        => $item->getData('payment_type'),
                                        'payment_base_amount' => $item->getData('base_row_payment_amount'),
                                    ]
                                );
                            } else {
                                $payments = [];
                                if ($item->getData('payment_id')) {
                                    array_push(
                                        $payments,
                                        [
                                            'payment_id'          => $item->getData('payment_id'),
                                            'payment_title'       => $item->getData('payment_title'),
                                            'payment_type'        => $item->getData('payment_type'),
                                            'payment_base_amount' => $item->getData('base_row_payment_amount'),
                                        ]
                                    );
                                }
                                $item->setData('payments', $payments);
                                $customer = $this->customerRepositoryInterface->getById($item->getData('customer_id'));
                                if ($customer) {
                                    $billingAddressId = $customer->getDefaultBilling();
                                    $item->setData('customer_email', $customer->getEmail());
                                    $item->setData('customer_name', $customer->getFirstname() . " " . $customer->getLastName());
                                    $item->setData('customer_phone', $billingAddressId ? $this->addressRepository->getById($billingAddressId)->getTelephone() : '');
                                    $item->setData('customer_group_code', $item->getData('customer_group_code'));
                                    $item->setData('data_report_type', $item->getData('order_item_id'));
                                }
                                $product = $this->productFactory->create()->load($item->getData('product_id'));
                                $item->setData('product_sku', $product->getSku());
                                $item->setData('product_unit', $product->getData('unit'));
                                $cats = $product->getCategoryIds();
                                if (count($cats)) {
                                    $firstCategoryId = $cats[count($cats) - 1];
                                    $_category = $this->categoryFactory->create()->load($firstCategoryId);
                                    $item->setData('category_name', $product->getData('master_category') ? $product->getData('master_category') : $_category->getName());
                                } else {
                                    $item->setData('category_name', $product->getData('master_category'));
                                }
                                $item->setData('store_view_code', $item->getStore()->getCode());
                                $xGroup['value'][] = $item->getData();
                            }
                        }
                    } else {
                        /** @var \Magento\Sales\Model\Order\Item $item */
                        foreach ($orderItems as $item) {
                            $xItem = $this->salesReportItemFactory->create();
                            $xItem->addData($item->getData());
                            $this->convertOutputData($data, $item, $xItem);
                            if ($xItem->getData('product_type') == "bundle"
                                || $xItem->getData('product_type') == "configurable"
                            ) {
                                $xItem->setData('total_cost', $item->getData('total_cost_for_parent_item'));
                            }
                            $grossProfit = $item->getData('revenue') - $item->getData('total_cost');
                            $xItem->setData('gross_profit', floatval($grossProfit));
                            if ($item->getData('revenue') != 0) {
                                $xItem->setData('margin', $grossProfit / ($item->getData('revenue')));
                            } else {
                                $xItem->setData('margin', $grossProfit / (1));
                            }
                            if ($item->getData('order_count') != 0) {
                                $xItem->setData(
                                    'cart_value',
                                    $item->getData('revenue') / ($item->getData('order_count'))
                                );
                            } else {
                                $xItem->setData(
                                    'cart_value',
                                    $item->getData('revenue') / (1)
                                );
                            }
                            if ($item->getData('order_count') != 0) {
                                $xItem->setData(
                                    'cart_value_incl_tax',
                                    $item->getData('grand_total') / ($item->getData('order_count'))
                                );
                            } else {
                                $xItem->setData(
                                    'cart_value_incl_tax',
                                    $item->getData('grand_total') / (1)
                                );
                            }
                            if (($item->getData('base_row_total_product') + $item->getData('discount_amount')) != 0) {
                                $xItem->setData(
                                    'discount_percent',
                                    $item->getData('discount_amount') / (($item->getData('base_row_total_product') + $item->getData('discount_amount')))
                                );
                            } else {
                                $xItem->setData(
                                    'discount_percent',
                                    $item->getData('discount_amount') / (1)
                                );
                            }
                            if ($item->getData('total_cart_size') != 0) {
                                $xItem->setData(
                                    'return_percent',
                                    $item->getData('total_refund_items') / ($item->getData('total_cart_size'))
                                );
                            } else {
                                $xItem->setData(
                                    'return_percent',
                                    $item->getData('total_refund_items') / (1)
                                );
                            }
                            $xItem->setData('return_count', $item->getData('total_refund_items'));
                            $xGroup['value'][] = $xItem->getData();
                        }
                    }
                }
                $group[] = $xGroup;
            }
        }

        return $group;
    }

    /**
     * XRT-5959
     *
     * @param      $dateStart
     * @param      $dateEnd
     * @param null $outletId
     *
     * @return array
     */
    protected function getDistributedTaxPercentAmounts($dateStart, $dateEnd, $outletId = null)
    {
        $data = $this->salesReportOrderItemCollectionFactory->create()
            ->getDistributedTaxPercentAmount($dateStart, $dateEnd, '', $outletId)
            ->getData();

        $results = [];

        foreach ($data as $d) {
            if (!isset($d['tax_percent']) || !isset($d['total_amount'])) {
                continue;
            }

            $idx = (string)$d['tax_percent'];
            $val = floatval($d['total_amount']);

            if (!isset($results[$idx])) {
                $results[$idx] = 0;
            }

            $results[$idx] += $val;
        }

        return $results;
    }

    /**
     * XRT-5959
     *
     * @param      $dateStart
     * @param      $dateEnd
     * @param      $grandTotal
     * @param null $outletId
     *
     * @return array
     */
    protected function getDistributedPaymentAmounts($dateStart, $dateEnd, $grandTotal, $outletId = null)
    {
        $data = $this->salesReportOrderCollectionFactory->create()
            ->getDistributedPaymentAmounts($dateStart, $dateEnd, $outletId)
            ->getData();

        $results = [];

        foreach ($data as $d) {
            $method = $d['method_code'];

            if ($method === 'payonaccount') {
                continue;
            }

            $additionalInformation = json_decode($d['additional_information'], true);
            $baseTotalInvoiced = floatval($d['base_total_invoiced']);
            $baseTotalRefunded = floatval($d['base_total_refunded']);

            if ($method !== 'retailmultiple') {
                if (!isset($results[$method])) {
                    $results[$method] = [
                        'name'   => !empty($additionalInformation['method_title']) ? $additionalInformation['method_title'] : ucfirst($method),
                        'amount' => 0,
                    ];
                }

                $results[$method]['amount'] += $baseTotalInvoiced - $baseTotalRefunded;
            } else {
                if (!isset($additionalInformation['split_data'])) {
                    continue;
                }
                $splitData = json_decode($additionalInformation['split_data'], true);
                $notAllowedTypes = [
                    RetailPayment::GIFT_CARD_PAYMENT_TYPE,
                    RetailPayment::REWARD_POINT_PAYMENT_TYPE,
                    RetailPayment::REFUND_GC_PAYMENT_TYPE,
                    RetailPayment::REFUND_TO_STORE_CREDIT_PAYMENT_TYPE,
                    RetailPayment::STORE_CREDIT_PAYMENT_TYPE,
                ];

                foreach ($splitData as $p) {
                    if (in_array($p['type'], $notAllowedTypes)) {
                        continue;
                    }

                    if ($p['title'] === 'Change') {
                        $p['title'] = 'Cash';
                    }

                    $method = $p['type'] . '_' . mb_eregi_replace(' ', '', mb_strtolower($p['title']));
                    $amount = floatval($p['amount']);

                    if (!isset($results[$method])) {
                        $results[$method] = [
                            'name'   => $p['title'],
                            'amount' => 0,
                        ];
                    }

                    if ($baseTotalInvoiced > 0) {
                        $results[$method]['amount'] += $amount;
                    }
                }
            }
        }

        return $results;
    }

    protected function getShippingAmount()
    {
    }

    protected function getSalesReportByOrderItem()
    {
        return $this->salesReportOrderItemCollectionFactory->create();
    }

    protected function getRetailPaymentMethodCollection($dateStart, $dateEnd, $outletId = null)
    {
        $collection = $this->transactionCollectionFactory->create();

        $dateRanger = $this->reportHelper->getReportOrderResource()->getDateRange('custom', $dateStart, $dateEnd);
        $collection->addFieldToFilter(
            'main_table.payment_type',
            [
                'nin' => [
                    RetailPayment::GIFT_CARD_PAYMENT_TYPE,
                    RetailPayment::REWARD_POINT_PAYMENT_TYPE,
                    RetailPayment::REFUND_GC_PAYMENT_TYPE,
                    RetailPayment::REFUND_TO_STORE_CREDIT_PAYMENT_TYPE,
                    RetailPayment::STORE_CREDIT_PAYMENT_TYPE,
                ],
            ]
        );
        $collection->addFieldToSelect('payment_id');
        $collection->getSelect()->group('payment_id');

        $collection->getSelect()->joinLeft(
            ['sorder' => $collection->getTable('sales_order')],
            'sorder.entity_id = main_table.order_id',
            ['order_id' => 'sorder.entity_id']
        );

        if ($outletId !== null && $outletId !== 'null') {
            $collection->addFieldToFilter('sorder.outlet_id', $outletId);
        }
        $collection->addFieldToFilter('sorder.created_at', $dateRanger);
        $collection->addFieldToFilter('sorder.retail_status', [['nin' => [11, 12, 13]], ['null' => true]]);
        $collection->getSelect()->columns(
            [
                'grand_total' => 'SUM(CASE WHEN main_table.base_amount = 0 THEN main_table.amount ELSE main_table.base_amount END)',
                'order_count' => 'COUNT(DISTINCT order_id)',
            ]
        );

        return $collection;
    }

    private function convertOutputData($dataFilter, $item, $xItem = null, $extra_info = null)
    {
        $reportType = $dataFilter['type'];
        switch ($reportType) {
            case "user":
                $data = $item->getData('user_id');
                $data_value = $item->getData('user_id');
                break;
            case "outlet":
                if ($dataFilter['item_filter']) {
                    $data = $item->getData('category_id');
                    $data_value = $item->getData('category_name');
                } else {
                    $data = $item->getData('outlet_id');
                    $data_value = $item->getData('name');
                }
                break;
            case "reference_number":
                $data = $item->getData('reference_number');
                $data_value = [
                    'name'   => $item->getData('reference_number'),
                    'outlet' => $item->getData('name'),
                ];
                break;
            case "region":
                if ($dataFilter['item_filter']) {
                    $data = $item->getData('customer_id');
                    $customer = $this->customerRepositoryInterface->getById($item->getData('customer_id'));
                    if ($customer) {
                        $data_value = $customer->getFirstname() . " " . $customer->getLastName();
                    } else {
                        $data_value = '';
                    }
                } else {
                    $data = $item->getData('region_id');
                    $data_value = $item->getData('region_name');
                }
                break;
            case "register":
                if ($dataFilter['item_filter']) {
                    $data = $item->getData('sku');
                    $data_value = $item->getData('name');
                    $totalShippingRegister = $item->getData('total_shipping_amount');
                } else {
                    $data = $item->getData('register_id');
                    $data_value = $item->getData('name');
                    $totalShippingRegister = $item->getData('total_shipping_amount');
                }
                break;
            case "customer":
                if ($dataFilter['item_filter']) {
                    $data = $item->getData('sku');
                    $data_value = $item->getData('name');
                } else {
                    $data = $item->getData('customer_id');
                    $customer = $this->customerRepositoryInterface->getById($item->getData('customer_id'));
                    if ($customer) {
                        $data_value = [
                            "email"                 => $customer->getEmail(),
                            "name"                  => $customer->getFirstname() . " " . $customer->getLastName(),
                            'phone'                 => $item->getData('customer_telephone'),
                            'customer_group_code'   => $item->getData('customer_group_code'),
                            'total_shipping_amount' => $item->getData('total_shipping_amount'),
                        ];
                    } else {
                        $data_value = [];
                    }
                }
                break;
            case "customer_group":
                $data = $item->getData('customer_group_id');
                $data_value = $item->getData('customer_group_code');
                break;
            case "magento_website":
                $data = $item->getData('website_id');
                $data_value = $item->getData('website_name');
                break;
            case "magento_storeview":
                $data = $item->getData('store_id');
                $data_value = $item->getData('store_name');
                break;
            case "payment_method":
                if ($dataFilter['item_filter']) {
                    $paymentMethod = $this->retailPaymentRepository->getById($item->getData('payment_id'));
                    $data = $item->getData('payment_id');
                    $data_value = $paymentMethod->getData('title');
                } else {
                    $data = $item->getData('payment_method');
                    $listPayment = $this->paymentHelper->getPaymentMethodList();
                    $data_value = isset($listPayment[$data]) ? $this->paymentHelper->getMethodInstance($data)->getTitle() . " ($data)" : $data;
                }
                break;
            case "shipping_method":
                $data = $item->getData('shipping_method');

                $data_value = $item->getData('shipping_description');
                break;
            case "order_status":
                if ($dataFilter['item_filter']) {
                    $data = $item->getData('status');
                    $data_value = $item->getData('status');
                } else {
                    $orderStatus = [
                        "33" => "ConnectPOS Partially Refund - Shipped",
                        "32" => "ConnectPOS Partially Refund - Not Shipped",
                        "31" => "ConnectPOS Partially Refund",
                        "40" => "ConnectPOS Fully Refunded",
                        "53" => "ConnectPOS Exchange - Shipped",
                        "52" => "ConnectPOS Exchange - Not Shipped",
                        "51" => "ConnectPOS Exchange",
                        "23" => "ConnectPOS Complete - Shipped",
                        "22" => "ConnectPOS Complete - Not Shipped",
                        "21" => "ConnectPOS Complete",
                        null => "Magento Status",
                    ];
                    if ($item->getData('retail_status') == null) {
                        $data = "magento_status";
                    } else {
                        $data = $item->getData('retail_status');
                    }
                    $data_value = $orderStatus[$item->getData('retail_status')];
                }
                break;
            case "currency":
                $data = $item->getData('order_currency_code');
                $data_value = $item->getData('order_currency_code');
                break;
            case "day_of_week":
                $weekDays = $this->localeLists->getOptionWeekdays();
                $data = $item->getData('day_of_week');
                foreach ($weekDays as $weekDay) {
                    if ($weekDay['value'] == intval($item->getData('day_of_week')) - 1) {
                        $data_value = $weekDay['label'];
                    }
                }
                break;
            case "hour":
                $data = $item->getData('hour');
                if (intval($item->getData('hour')) >= 12) {
                    $hour = intval($item->getData('hour')) - 12;
                    if (intval($item->getData('hour')) == 12) {
                        $data_value = intval($item->getData('hour')) . ' pm - ' . ($hour + 1) . ' pm';
                    } elseif (intval($item->getData('hour')) == 23) {
                        $data_value = $hour . ' pm - ' . ($hour + 1) . ' am';
                    } else {
                        $data_value = $hour . ' pm - ' . ($hour + 1) . ' pm';
                    }
                } else {
                    if (intval($item->getData('hour')) == 11) {
                        $data_value = intval($item->getData('hour')) . ' am - ' . (intval($item->getData('hour')) + 1) . ' pm';
                    } elseif (intval($item->getData('hour')) == 0) {
                        $data_value = (intval($item->getData('hour')) + 12) . ' am - ' . (intval($item->getData('hour')) + 1) . ' am';
                    } else {
                        $data_value = intval($item->getData('hour')) . ' am - ' . (intval($item->getData('hour')) + 1) . ' am';
                    }
                }
                break;
            case "product":
                $data = $item->getData('sku');
                $productModel = $this->productFactory->create()->loadByAttribute('sku', $item->getData('sku'));
                $product_name = '';
                if ($productModel) {
                    $product_name = $productModel->getData('name');
                } else {
                    $arrayName = explode(",", $item->getData('all_product_name'));
                    if (is_array($arrayName) && end($arrayName)) {
                        $product_name = end($arrayName);
                    }
                }
                $data_value = [
                    'name'         => $product_name,
                    'sku'          => $item->getData('sku'),
                    'product_type' => $item->getData('product_type'),
                    'manufacturer' => $item->getData('manufacturer_value'),
                ];
                break;
            case "manufacturer":
                $data = $item->getData("manufacturer_key");
                $data_value = $item->getData('manufacturer_value');
                break;
            case "category":
                if ($dataFilter['item_filter']) {
                    switch ($extra_info) {
                        case 'outlet':
                            $data = $item->getData('outlet_id');
                            $data_value = $item->getData('name');
                            break;
                        case 'region':
                            $data = $item->getData('region_id');
                            $data_value = $item->getData('region_name');
                            break;
                        default:
                            $data = $item->getData("name");
                            $data_value = $item->getData("name");
                            break;
                    }
                } else {
                    $data = $item->getData("category_id");
                    $data_value = $item->getData('category_name');
                }
                break;
            case "monetary":
                $data = $item->getData("order_id");
                $data_value = $item->getData('created_at');
                break;
            case "order_item":
                $data = $item->getData("order_item_id");
                $data_value = 'Bán lẻ';
                break;
            default:
                $data = $item->getData("name");
                $data_value = $item->getData("name");
                break;
        }
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
            if ($reportType == "register") {
                return ["data" => $data, "value" => $data_value, "total_shipping_amount" => $totalShippingRegister];
            }

            return ["data" => $data, "value" => $data_value];
        }
    }

    private function addSqlFilterItem($dataFilter, $field_filter, $collection)
    {
        if ((in_array('N/A', $dataFilter))
            || (in_array('magento_status', $dataFilter))
        ) {
            $collection->addFieldToFilter(
                $field_filter,
                [
                    ['in' => $dataFilter],
                    ['null' => true],
                ]
            );
        } else {
            $collection->addAttributeToFilter($field_filter, ['in' => $dataFilter]);
        }

        return $collection;
    }

    private function addDataFilterItems($reportType, $dataGroupBy, $collection)
    {
        $dataFilter = [];
        foreach ($dataGroupBy as $filter) {
            $dataFilter[] = $filter['data'];
        }
        $reportType = $reportType['type'];
        switch ($reportType) {
            case "user":
                $this->addSqlFilterItem($dataFilter, 'user_id', $collection);
                break;
            case "outlet":
                $this->addSqlFilterItem($dataFilter, 'outlet_id', $collection);
                break;
            case "region":
                $this->addSqlFilterItem($dataFilter, 'region_id', $collection);
                break;
            case "reference_number":
                $this->addSqlFilterItem($dataFilter, 'reference_number', $collection);
                break;
            case "register":
                $this->addSqlFilterItem($dataFilter, 'register_id', $collection);
                break;
            case "customer":
                $this->addSqlFilterItem($dataFilter, 'customer_id', $collection);
                break;
            case "customer_group":
                $this->addSqlFilterItem($dataFilter, 'main_table.customer_group_id', $collection);
                break;
            case "magento_website":
                $this->addSqlFilterItem($dataFilter, 'website_int.website_id', $collection);
                break;
            case "magento_storeview":
                $this->addSqlFilterItem($dataFilter, 'main_table.store_id', $collection);
                break;
            case "payment_method":
                $this->addSqlFilterItem($dataFilter, 'spayment.method', $collection);
                break;
            case "shipping_method":
                $this->addSqlFilterItem($dataFilter, 'shipping_method', $collection);
                break;
            case "order_status":
                $this->addSqlFilterItem($dataFilter, 'retail_status', $collection);
                break;
            case "currency":
                $this->addSqlFilterItem($dataFilter, 'order_currency_code', $collection);
                break;
            case "day_of_week":
                foreach ($dataFilter as $day) {
                    $collection->getSelect()->orhaving("`day_of_week` " . '=' . ' ?', $day);
                }
                break;
            case "hour":
                foreach ($dataFilter as $day) {
                    $collection->getSelect()->orhaving("`hour` " . '=' . ' ?', $day);
                }
                break;
            case "product":
                $this->addSqlFilterItem($dataFilter, 'sku', $collection);
                break;
            case "manufacturer":
                $this->addSqlFilterItem($dataFilter, 'product_int.value', $collection);
                break;
            case "category":
                $this->addSqlFilterItem($dataFilter, 'category_id', $collection);
                break;
            default:
                break;
        }

        return $collection;
    }
}
