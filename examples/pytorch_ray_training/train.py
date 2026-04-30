"""Single-node PyTorch MNIST training using Ray Train - Coldpress will distribute this."""

import os
import json
import time
import argparse
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, random_split
import torchvision
import torchvision.transforms as transforms

import ray
from ray import train
from ray.train import ScalingConfig, RunConfig, CheckpointConfig
from ray.train.torch import TorchTrainer


class DynamicMLP(nn.Module):
    """Simple MLP for classification."""

    def __init__(self, input_dim, hidden_size, output_dim):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(input_dim, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, output_dim),
        )

    def forward(self, x):
        return self.layers(x)


def train_func(config):
    """Training function for Ray Train - runs on each worker.

    Ray Train automatically handles:
    - Device placement
    - Model DDP wrapping
    - Data distribution
    """
    # Get hyperparameters
    dataset = config.get("dataset", "mnist")
    train_test_split = config.get("train_test_split", 0.8)
    epochs = config.get("epochs", 10)
    batch_size = config.get("batch_size", 64)
    hidden_size = config.get("hidden_size", 128)
    lr = config.get("lr", 0.01)
    output_dir = config.get("output_dir", "/results")

    # Ray Train context
    rank = train.get_context().get_world_rank()
    world_size = train.get_context().get_world_size()

    # Dataset loading - shared storage
    dataset_root = "/results/datasets"
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Lambda(lambda x: torch.flatten(x))]
    )

    # Only rank 0 downloads
    if rank == 0:
        if dataset.lower() == "mnist":
            torchvision.datasets.MNIST(
                root=dataset_root, train=True, download=True, transform=transform
            )
        else:
            raise ValueError("Unsupported dataset")

    # Wait for download
    torch.distributed.barrier()

    # All ranks load
    if dataset.lower() == "mnist":
        full_dataset = torchvision.datasets.MNIST(
            root=dataset_root, train=True, download=False, transform=transform
        )
    else:
        raise ValueError("Unsupported dataset")

    input_dim = full_dataset[0][0].numel()
    output_dim = len(full_dataset.classes) if hasattr(full_dataset, "classes") else 10

    # Train-test split
    train_size = int(train_test_split * len(full_dataset))
    test_size = len(full_dataset) - train_size
    generator = torch.Generator().manual_seed(42)
    train_dataset, test_dataset = random_split(
        full_dataset, [train_size, test_size], generator=generator
    )

    # Data loaders
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=batch_size)

    # Ray Train handles device placement automatically
    train_loader = train.torch.prepare_data_loader(train_loader)
    test_loader = train.torch.prepare_data_loader(test_loader)

    # Model - Ray Train wraps in DDP
    model = DynamicMLP(input_dim, hidden_size, output_dim)
    model = train.torch.prepare_model(model)

    optimizer = optim.SGD(model.parameters(), lr=lr)
    criterion = nn.CrossEntropyLoss()

    # Training loop
    start = time.time()
    accuracy = 0.0

    for epoch in range(epochs):
        model.train()
        running_loss = 0.0

        for data, target in train_loader:
            # Ray Train handles device placement
            optimizer.zero_grad()
            output = model(data)
            loss = criterion(output, target)
            loss.backward()
            optimizer.step()
            running_loss += loss.item()

        # Evaluate every 10 epochs
        if (epoch + 1) % 10 == 0 or (epoch + 1) == epochs:
            model.eval()
            correct = 0
            total = 0
            with torch.no_grad():
                for data, target in test_loader:
                    outputs = model(data)
                    _, predicted = torch.max(outputs.data, 1)
                    total += target.size(0)
                    correct += (predicted == target).sum().item()

            avg_loss = running_loss / len(train_loader)
            accuracy = 100 * correct / total

            if rank == 0:
                print(
                    f"Epoch [{epoch + 1}/{epochs}] - Train Loss: {avg_loss:.4f} - Test Accuracy: {accuracy:.2f}%",
                    flush=True,
                )

            # Report to Ray Train
            train.report(
                {
                    "epoch": epoch + 1,
                    "loss": avg_loss,
                    "accuracy": accuracy,
                }
            )

    end = time.time()

    # Save results (rank 0 only)
    if rank == 0:
        num_params = sum(p.numel() for p in model.parameters())

        results = {
            "dataset": dataset,
            "epochs": epochs,
            "batch_size": batch_size,
            "hidden_size": hidden_size,
            "train_test_split": train_test_split,
            "num_gpus": world_size,
            "time_seconds": end - start,
            "final_loss": float(loss),
            "accuracy": accuracy,
            "model_params": num_params,
            "input_dim": input_dim,
            "output_dim": output_dim,
            "framework": "ray_train",
        }

        os.makedirs(output_dir, exist_ok=True)

        with open(f"{output_dir}/training_stats.json", "w") as f:
            json.dump(results, f, indent=2)

        # Save model
        model_to_save = model.module if hasattr(model, "module") else model
        torch.save(model_to_save.state_dict(), f"{output_dir}/model_weights.pth")
        print(f"Saved model weights to {output_dir}/model_weights.pth", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, default="mnist")
    parser.add_argument("--train-test-split", type=float, default=0.8)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--hidden-size", type=int, default=128)
    parser.add_argument("--lr", type=float, default=0.01)
    parser.add_argument("--output-dir", type=str, default="/results")
    # Ray Train scaling config - passed from bash wrapper
    parser.add_argument("--num-workers", type=int, default=1)
    parser.add_argument("--gpus-per-worker", type=int, default=2)
    parser.add_argument("--cpus-per-worker", type=int, default=4)
    args = parser.parse_args()

    # Initialize Ray (connects to local Ray or existing cluster)
    ray.init()

    # Configure for training
    # Single-node default: 1 worker with 2 GPUs
    # Distributed: bash wrapper reads env vars and passes as args
    scaling_config = ScalingConfig(
        num_workers=args.num_workers,
        use_gpu=True,
        resources_per_worker={"GPU": args.gpus_per_worker, "CPU": args.cpus_per_worker},
    )

    checkpoint_config = CheckpointConfig(
        num_to_keep=1,
        checkpoint_score_attribute="accuracy",
        checkpoint_score_order="max",
    )

    run_config = RunConfig(
        name="mnist-training",
        storage_path=args.output_dir,
        checkpoint_config=checkpoint_config,
    )

    # Create trainer
    trainer = TorchTrainer(
        train_func,
        train_loop_config={
            "dataset": args.dataset,
            "train_test_split": args.train_test_split,
            "epochs": args.epochs,
            "batch_size": args.batch_size,
            "hidden_size": args.hidden_size,
            "lr": args.lr,
            "output_dir": args.output_dir,
        },
        scaling_config=scaling_config,
        run_config=run_config,
    )

    # Run training
    result = trainer.fit()

    print("\nTraining completed!")
    print(f"Best checkpoint: {result.checkpoint}")
    print(f"Final metrics: {result.metrics}")

    ray.shutdown()


if __name__ == "__main__":
    main()
