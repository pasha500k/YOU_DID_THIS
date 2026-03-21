const nbt = require("prismarine-nbt");

const formattingCodePattern = /§[0-9A-FK-ORX]/giu;

function stripFormattingCodes(value) {
  return String(value || "").replace(formattingCodePattern, "");
}

function flattenComponent(component) {
  if (component === null || component === undefined) {
    return "";
  }

  if (typeof component === "string") {
    const trimmed = component.trim();
    if (
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]"))
    ) {
      try {
        return flattenComponent(JSON.parse(trimmed));
      } catch {
        return stripFormattingCodes(trimmed);
      }
    }

    return stripFormattingCodes(component);
  }

  if (Array.isArray(component)) {
    return component.map(flattenComponent).join("");
  }

  if (typeof component === "object") {
    const parts = [];

    if (component.text) {
      parts.push(flattenComponent(component.text));
    }

    if (component.translate) {
      parts.push(flattenComponent(component.translate));
    }

    if (Array.isArray(component.extra)) {
      parts.push(component.extra.map(flattenComponent).join(""));
    }

    return parts.join("");
  }

  return String(component);
}

function cleanText(value) {
  return flattenComponent(value).replace(/\s+/g, " ").trim();
}

function simplifyItemNbt(item) {
  if (!item || !item.nbt) {
    return {};
  }

  try {
    return nbt.simplify(item.nbt) || {};
  } catch {
    return {};
  }
}

function extractEnchantments(rawNbt) {
  const candidates = rawNbt.Enchantments || rawNbt.ench || [];
  if (!Array.isArray(candidates)) {
    return [];
  }

  return candidates
    .map((entry) => {
      const id = entry.id || entry.ID || entry.Name || "unknown";
      const level = entry.lvl || entry.level || entry.Level || 0;
      return `${id}:${level}`;
    })
    .filter(Boolean);
}

function extractItemMeta(item) {
  const rawNbt = simplifyItemNbt(item);
  const display = rawNbt.display || {};
  const lore = Array.isArray(display.Lore)
    ? display.Lore.map(cleanText).filter(Boolean)
    : [];
  const customName =
    cleanText(display.Name) ||
    cleanText(item.customName) ||
    cleanText(item.displayName) ||
    cleanText(item.name);

  return {
    cleanName: customName,
    lore,
    enchantments: extractEnchantments(rawNbt),
    rawNbt,
  };
}

module.exports = {
  cleanText,
  extractItemMeta,
};
