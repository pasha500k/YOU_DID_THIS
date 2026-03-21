const OpenAI = require("openai");

function buildAiInput(financialModel, config) {
  return {
    snapshot: financialModel.snapshot,
    topValueGroups: financialModel.groups.slice(0, config.auction.maxGroupsForAi),
    topOpportunities: financialModel.opportunities.slice(
      0,
      config.auction.maxOpportunitiesForAi,
    ),
  };
}

async function generateOpenAiReport(financialModel, config) {
  const client = new OpenAI({
    apiKey: config.openai.apiKey,
  });

  const aiInput = buildAiInput(financialModel, config);
  const response = await client.responses.create({
    model: config.openai.model,
    input: [
      {
        role: "system",
        content: [
          {
            type: "input_text",
            text:
              "Ты аналитик игрового аукциона Minecraft. Пиши только по данным из JSON, не выдумывай отсутствующие факты. Ответ дай на русском языке в Markdown.",
          },
        ],
      },
      {
        role: "user",
        content: [
          {
            type: "input_text",
            text:
              "Сделай финансовую модель рынка с разделами: 1) структура рынка, 2) справедливые цены и диапазоны, 3) недооценённые лоты, 4) риски и ликвидность, 5) практический план действий на 1-2 цикла торговли. JSON данных:\n" +
              JSON.stringify(aiInput, null, 2),
          },
        ],
      },
    ],
  });

  return response.output_text?.trim() || "";
}

module.exports = {
  generateOpenAiReport,
};
