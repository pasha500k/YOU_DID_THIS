function round(value) {
  return Math.round(value * 100) / 100;
}

function percentile(sortedValues, fraction) {
  if (sortedValues.length === 0) {
    return 0;
  }

  if (sortedValues.length === 1) {
    return sortedValues[0];
  }

  const index = (sortedValues.length - 1) * fraction;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  const weight = index - lower;

  if (lower === upper) {
    return sortedValues[lower];
  }

  return (
    sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight
  );
}

function average(values) {
  if (values.length === 0) {
    return 0;
  }

  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function standardDeviation(values, mean) {
  if (values.length <= 1) {
    return 0;
  }

  const variance =
    values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

function liquidityLabel(sampleSize, sellerCount, volatilityPct) {
  if (sampleSize >= 10 && sellerCount >= 4 && volatilityPct <= 18) {
    return "high";
  }

  if (sampleSize >= 5 && sellerCount >= 2 && volatilityPct <= 35) {
    return "medium";
  }

  return "low";
}

function confidenceScore(sampleSize, volatilityPct) {
  const sampleComponent = Math.min(sampleSize / 12, 1);
  const volatilityComponent = Math.max(0, 1 - volatilityPct / 80);
  return round((sampleComponent * 0.65 + volatilityComponent * 0.35) * 100);
}

function buildGroupSummary(key, listings) {
  const prices = listings
    .map((listing) => listing.price)
    .filter((price) => Number.isFinite(price))
    .sort((a, b) => a - b);

  const sellers = listings
    .map((listing) => listing.seller)
    .filter(Boolean);
  const sellerCounts = new Map();
  for (const seller of sellers) {
    sellerCounts.set(seller, (sellerCounts.get(seller) || 0) + 1);
  }

  const sampleSize = prices.length;
  const median = percentile(prices, 0.5);
  const p25 = percentile(prices, 0.25);
  const p75 = percentile(prices, 0.75);
  const mean = average(prices);
  const min = prices[0] || 0;
  const max = prices[prices.length - 1] || 0;
  const stdev = standardDeviation(prices, mean);
  const volatilityPct = mean > 0 ? round((stdev / mean) * 100) : 0;
  const spreadPct = median > 0 ? round(((p75 - p25) / median) * 100) : 0;
  const topSellerShare =
    sampleSize > 0 && sellerCounts.size > 0
      ? round((Math.max(...sellerCounts.values()) / sampleSize) * 100)
      : 0;

  return {
    key,
    displayName: listings[0]?.displayName || key,
    itemName: listings[0]?.itemName || "unknown",
    sampleSize,
    sellerCount: sellerCounts.size,
    minPrice: min,
    maxPrice: max,
    averagePrice: round(mean),
    medianPrice: round(median),
    buyZone: round(p25),
    sellZone: round(p75),
    spreadPct,
    volatilityPct,
    topSellerSharePct: topSellerShare,
    liquidity: liquidityLabel(sampleSize, sellerCounts.size, volatilityPct),
    confidenceScore: confidenceScore(sampleSize, volatilityPct),
    totalVisibleVolume: prices.reduce((sum, value) => sum + value, 0),
  };
}

function buildOpportunity(listing, group, config) {
  if (
    !group ||
    group.sampleSize < config.auction.minSample ||
    !Number.isFinite(listing.price) ||
    !Number.isFinite(group.medianPrice) ||
    group.medianPrice <= 0
  ) {
    return null;
  }

  const exitPrice = group.medianPrice;
  const taxAmount = exitPrice * (config.auction.taxPercent / 100);
  const netExitPrice = exitPrice - taxAmount;
  const profit = netExitPrice - listing.price;
  const roiPct = listing.price > 0 ? round((profit / listing.price) * 100) : 0;
  const discountPct = round(
    ((group.medianPrice - listing.price) / group.medianPrice) * 100,
  );

  if (profit < config.auction.minProfit || roiPct < config.auction.minRoiPercent) {
    return null;
  }

  return {
    normalizedName: listing.normalizedName,
    displayName: listing.displayName,
    price: listing.price,
    quantity: listing.quantity,
    seller: listing.seller,
    pageNumber: listing.pageNumber,
    slot: listing.slot,
    expectedExitPrice: round(exitPrice),
    taxPercent: config.auction.taxPercent,
    expectedNetExitPrice: round(netExitPrice),
    projectedProfit: round(profit),
    projectedRoiPct: roiPct,
    discountToMedianPct: discountPct,
    liquidity: group.liquidity,
    confidenceScore: group.confidenceScore,
  };
}

function buildFinancialModel(scanResult, config) {
  const pricedListings = scanResult.listings.filter((listing) =>
    Number.isFinite(listing.price),
  );
  const groupedListings = new Map();

  for (const listing of pricedListings) {
    const bucket = groupedListings.get(listing.normalizedName) || [];
    bucket.push(listing);
    groupedListings.set(listing.normalizedName, bucket);
  }

  const groups = Array.from(groupedListings.entries())
    .map(([key, listings]) => buildGroupSummary(key, listings))
    .sort((left, right) => right.totalVisibleVolume - left.totalVisibleVolume);

  const groupsByKey = new Map(groups.map((group) => [group.key, group]));
  const opportunities = pricedListings
    .map((listing) =>
      buildOpportunity(listing, groupsByKey.get(listing.normalizedName), config),
    )
    .filter(Boolean)
    .sort((left, right) => {
      if (right.projectedProfit !== left.projectedProfit) {
        return right.projectedProfit - left.projectedProfit;
      }

      return right.projectedRoiPct - left.projectedRoiPct;
    });

  const topValueGroups = groups.slice(0, 10);
  const highConfidenceGroups = groups
    .filter((group) => group.confidenceScore >= 60)
    .slice(0, 10);

  return {
    generatedAt: new Date().toISOString(),
    snapshot: {
      scannedAt: scanResult.scannedAt,
      pagesScanned: scanResult.pages.length,
      totalListings: scanResult.listings.length,
      pricedListings: pricedListings.length,
      uniquePricedItems: groups.length,
      totalVisibleMarketValue: pricedListings.reduce(
        (sum, listing) => sum + listing.price,
        0,
      ),
      averageListingPrice: round(
        average(pricedListings.map((listing) => listing.price)),
      ),
      taxPercent: config.auction.taxPercent,
    },
    groups,
    opportunities,
    highlights: {
      topValueGroups,
      highConfidenceGroups,
      topOpportunities: opportunities.slice(0, 15),
    },
  };
}

function formatCoins(value) {
  return new Intl.NumberFormat("ru-RU").format(Math.round(value || 0));
}

function renderFinancialModelMarkdown(financialModel) {
  const lines = [
    "# Финансовая модель рынка",
    "",
    `Снимок: ${financialModel.snapshot.scannedAt}`,
    `Страниц: ${financialModel.snapshot.pagesScanned}`,
    `Всего лотов: ${financialModel.snapshot.totalListings}`,
    `Лотов с ценой: ${financialModel.snapshot.pricedListings}`,
    `Уникальных позиций: ${financialModel.snapshot.uniquePricedItems}`,
    `Видимый объём рынка: ${formatCoins(financialModel.snapshot.totalVisibleMarketValue)}`,
    `Средняя цена лота: ${formatCoins(financialModel.snapshot.averageListingPrice)}`,
    `Налог модели: ${financialModel.snapshot.taxPercent}%`,
    "",
    "## Топ возможностей",
    "",
  ];

  if (financialModel.highlights.topOpportunities.length === 0) {
    lines.push("Подходящих сделок по текущим порогам не найдено.");
  } else {
    for (const opportunity of financialModel.highlights.topOpportunities.slice(0, 10)) {
      lines.push(
        `- ${opportunity.displayName}: вход ${formatCoins(opportunity.price)}, ` +
          `выход ${formatCoins(opportunity.expectedNetExitPrice)}, ` +
          `прибыль ${formatCoins(opportunity.projectedProfit)}, ROI ${opportunity.projectedRoiPct}%`,
      );
    }
  }

  lines.push("", "## Крупнейшие сегменты", "");

  for (const group of financialModel.highlights.topValueGroups.slice(0, 10)) {
    lines.push(
      `- ${group.displayName}: медиана ${formatCoins(group.medianPrice)}, ` +
        `зона покупки ${formatCoins(group.buyZone)}, зона продажи ${formatCoins(group.sellZone)}, ` +
        `ликвидность ${group.liquidity}, confidence ${group.confidenceScore}/100`,
    );
  }

  return lines.join("\n");
}

module.exports = {
  buildFinancialModel,
  renderFinancialModelMarkdown,
};
