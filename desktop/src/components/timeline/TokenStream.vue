<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import DOMPurify from "dompurify";
import { marked } from "marked";

const props = defineProps<{ tokens: string[]; finalText?: string; streaming?: boolean }>();
const text = computed(() => props.finalText || props.tokens.join(""));
const renderedText = ref("");
let renderTimer: number | undefined;

function clearRenderTimer() {
  if (renderTimer === undefined) return;
  window.clearTimeout(renderTimer);
  renderTimer = undefined;
}

watch([text, () => props.streaming], ([value, streaming]) => {
  if (!streaming || !value || !renderedText.value) {
    clearRenderTimer();
    renderedText.value = value;
    return;
  }
  if (renderTimer !== undefined) return;
  renderTimer = window.setTimeout(() => {
    renderTimer = undefined;
    renderedText.value = text.value;
  }, 80);
}, { immediate: true });

const html = computed(() => DOMPurify.sanitize(marked.parse(renderedText.value, { async: false }) as string));

onBeforeUnmount(clearRenderTimer);
</script>

<template>
  <div v-if="text" class="token-stream markdown-body" :class="{ streaming }">
    <div v-html="html" />
    <i v-if="streaming" />
  </div>
</template>
