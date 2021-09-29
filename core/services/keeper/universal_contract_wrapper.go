package keeper

import (
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/pkg/errors"
	"github.com/smartcontractkit/chainlink/core/internal/gethwrappers/generated"
	"github.com/smartcontractkit/chainlink/core/internal/gethwrappers/generated/keeper_registry_wrapper"
	"github.com/smartcontractkit/chainlink/core/internal/gethwrappers/generated/keeper_registry_wrapper_v1_0_0"
	"github.com/smartcontractkit/chainlink/core/services/log"
)

type Version int64

type GetUpkeep struct {
	Target              common.Address
	ExecuteGas          uint32
	CheckData           []byte
	Balance             *big.Int
	LastKeeper          common.Address
	Admin               common.Address
	MaxValidBlocknumber uint64
}

type GetConfig struct {
	PaymentPremiumPPB    uint32
	FlatFeeMicroLink     uint32
	BlockCountPerTurn    *big.Int
	CheckGasLimit        uint32
	StalenessSeconds     *big.Int
	GasCeilingMultiplier uint16
	FallbackGasPrice     *big.Int
	FallbackLinkPrice    *big.Int
}

const (
	v1 Version = iota
	v2
)

// UniversalContractWrapper is a common interface for different versions of the keeper registry
// DEV: this is meant as only a temporary solution to the multi-version contract problem
type UniversalContractWrapper interface {
	Address() common.Address
	ParseLog(log types.Log) (generated.AbigenLog, error)
	LogsWithTopics() map[common.Hash][][]log.Topic
	GetUpkeepCount(opts *bind.CallOpts) (*big.Int, error)
	GetUpkeep(opts *bind.CallOpts, id *big.Int) (GetUpkeep, error)
	GetCanceledUpkeepList(opts *bind.CallOpts) ([]*big.Int, error)
	GetKeeperList(opts *bind.CallOpts) ([]common.Address, error)
	GetConfig(opts *bind.CallOpts) (GetConfig, error)
}

var _ UniversalContractWrapper = universalContractWrapper{}

//  NewUniversalContractWrapper creates a new UniversalContractWrapper
func NewUniversalContractWrapper(
	contractVersion string,
	address common.Address,
	backend bind.ContractBackend,
) (UniversalContractWrapper, error) {
	switch contractVersion {
	case "1.0.0", "":
		contractV1, err := keeper_registry_wrapper_v1_0_0.NewKeeperRegistry(address, backend)
		if err != nil {
			return nil, err
		}
		return universalContractWrapper{
			version:    v1,
			contractV1: contractV1,
		}, nil
	case "2.0.0", "latest":
		contractV2, err := keeper_registry_wrapper.NewKeeperRegistry(address, backend)
		if err != nil {
			return nil, err
		}
		return universalContractWrapper{
			version:    v2,
			contractV2: contractV2,
		}, nil
	default:
		return nil, errors.Errorf("invalid contract version: %s", contractVersion)
	}
}

type universalContractWrapper struct {
	version    Version
	contractV1 *keeper_registry_wrapper_v1_0_0.KeeperRegistry
	contractV2 *keeper_registry_wrapper.KeeperRegistry
}

func (uw universalContractWrapper) Address() common.Address {
	switch uw.version {
	case v1:
		return uw.contractV1.Address()
	case v2:
		return uw.contractV2.Address()
	}
	panic("invariant")
}

func (uw universalContractWrapper) ParseLog(log types.Log) (generated.AbigenLog, error) {
	switch uw.version {
	case v1:
		return uw.contractV1.ParseLog(log)
	case v2:
		return uw.contractV2.ParseLog(log)
	}
	panic("invariant")
}

func (uw universalContractWrapper) LogsWithTopics() map[common.Hash][][]log.Topic {
	switch uw.version {
	case v1:
		return map[common.Hash][][]log.Topic{
			keeper_registry_wrapper_v1_0_0.KeeperRegistryKeepersUpdated{}.Topic():   nil,
			keeper_registry_wrapper_v1_0_0.KeeperRegistryConfigSet{}.Topic():        nil,
			keeper_registry_wrapper_v1_0_0.KeeperRegistryUpkeepCanceled{}.Topic():   nil,
			keeper_registry_wrapper_v1_0_0.KeeperRegistryUpkeepRegistered{}.Topic(): nil,
			keeper_registry_wrapper_v1_0_0.KeeperRegistryUpkeepPerformed{}.Topic():  nil,
		}
	case v2:
		return map[common.Hash][][]log.Topic{
			keeper_registry_wrapper.KeeperRegistryKeepersUpdated{}.Topic():   nil,
			keeper_registry_wrapper.KeeperRegistryConfigSet{}.Topic():        nil,
			keeper_registry_wrapper.KeeperRegistryUpkeepCanceled{}.Topic():   nil,
			keeper_registry_wrapper.KeeperRegistryUpkeepRegistered{}.Topic(): nil,
			keeper_registry_wrapper.KeeperRegistryUpkeepPerformed{}.Topic():  nil,
		}
	}
	panic("invariant")
}

func (uw universalContractWrapper) GetUpkeepCount(opts *bind.CallOpts) (*big.Int, error) {
	switch uw.version {
	case v1:
		return uw.contractV1.GetUpkeepCount(opts)
	case v2:
		return uw.contractV2.GetUpkeepCount(opts)
	}
	panic("invariant")
}

func (uw universalContractWrapper) GetUpkeep(opts *bind.CallOpts, id *big.Int) (GetUpkeep, error) {
	switch uw.version {
	case v1:
		upkeep, err := uw.contractV1.GetUpkeep(opts, id)
		return GetUpkeep(upkeep), err
	case v2:
		upkeep, err := uw.contractV2.GetUpkeep(opts, id)
		return GetUpkeep(upkeep), err
	}
	panic("invariant")
}

func (uw universalContractWrapper) GetCanceledUpkeepList(opts *bind.CallOpts) ([]*big.Int, error) {
	switch uw.version {
	case v1:
		return uw.contractV1.GetCanceledUpkeepList(opts)
	case v2:
		return uw.contractV2.GetCanceledUpkeepList(opts)
	}
	panic("invariant")
}

func (uw universalContractWrapper) GetKeeperList(opts *bind.CallOpts) ([]common.Address, error) {
	switch uw.version {
	case v1:
		return uw.contractV1.GetKeeperList(opts)
	case v2:
		return uw.contractV2.GetKeeperList(opts)
	}
	panic("invariant")
}

func (uw universalContractWrapper) GetConfig(opts *bind.CallOpts) (GetConfig, error) {
	switch uw.version {
	case v1:
		// XXX: this only works because we don't use config values that changed b/t versions 1 and 2
		upkeep, err := uw.contractV1.GetConfig(opts)
		return GetConfig(upkeep), err
	case v2:
		upkeep, err := uw.contractV2.GetConfig(opts)
		return GetConfig(upkeep), err
	}
	panic("invariant")
}
