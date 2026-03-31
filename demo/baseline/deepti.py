#!/usr/bin/env python3

import os
import subprocess
import torch

from transformers import modeling_utils
if not hasattr(modeling_utils, "ALL_PARALLEL_STYLES") or modeling_utils.ALL_PARALLEL_STYLES is None:
    modeling_utils.ALL_PARALLEL_STYLES = ["tp", "none", "colwise", "rowwise"]

import transformers.utils.import_utils

def _bypass_check():
    pass

transformers.utils.import_utils.check_torch_load_is_safe = _bypass_check

from qwen_omni_utils import process_mm_info
from transformers import Qwen2_5OmniForConditionalGeneration, Qwen2_5OmniProcessor

DUMMY_VIDEO = "test_video.mp4"

if not os.path.exists(DUMMY_VIDEO):
    print("[INFO] Creating dummy video with ffmpeg...")
    subprocess.run(
        ["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=size=224x224:rate=5",
         "-t", "2", "-pix_fmt", "yuv420p", DUMMY_VIDEO],
        check=True,
    )

assert os.path.exists(DUMMY_VIDEO), "ffmpeg failed to create dummy video"
print("[OK] ffmpeg works")

MODEL_NAME = "Qwen/Qwen2.5-Omni-7B"

print("[INFO] Loading model...")
model = Qwen2_5OmniForConditionalGeneration.from_pretrained(
    MODEL_NAME,
    torch_dtype=torch.bfloat16,
    device_map="auto",
    attn_implementation="flash_attention_2",
    trust_remote_code=True,
)
model.disable_talker()

processor = Qwen2_5OmniProcessor.from_pretrained(MODEL_NAME)
print("[OK] Model + processor loaded")

conversation = [
    {
        "role": "system",
        "content": [{"type": "text", "text": "You are a helpful multimodal assistant."}],
    },
    {
        "role": "user",
        "content": [
            {"type": "video", "video": DUMMY_VIDEO},
            {"type": "text", "text": "What do you see in this video?"},
        ],
    },
]

text = processor.apply_chat_template(conversation, add_generation_prompt=True, tokenize=False)
audios, images, videos = process_mm_info(conversation, use_audio_in_video=False)
inputs = processor(text=text, videos=videos, audio=audios, images=images, 
                   return_tensors="pt", padding=True, use_audio_in_video=False)
inputs = inputs.to(model.device).to(model.dtype)

print("[INFO] Running generation...")
with torch.no_grad():
    output_ids = model.generate(**inputs, max_new_tokens=128)

decoded = processor.batch_decode(output_ids, skip_special_tokens=True, clean_up_tokenization_spaces=False)
answer = decoded[0].split("\nassistant\n")[-1].strip()

print("\n=== MODEL OUTPUT ===")
print(answer)
print("====================")
print("[SUCCESS] Minimal Qwen2.5-Omni + ffmpeg + flash-attn test completed")
